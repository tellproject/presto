package ch.ethz.kudu

import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider
import com.facebook.presto.spi.connector.ConnectorSplitManager
import com.facebook.presto.spi.connector.ConnectorTransactionHandle
import com.facebook.presto.spi.type.BigintType
import com.facebook.presto.spi.type.BooleanType
import com.facebook.presto.spi.type.Type
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.stumbleupon.async.Deferred
import io.airlift.slice.Slice
import io.airlift.slice.Slices
import org.kududb.Type.*
import org.kududb.client.*
import java.util.concurrent.CompletableFuture

class KuduTransactionHandle : ConnectorTransactionHandle

class KuduRecordCursor(val scanner: AsyncKuduScanner, val columns: MutableList<out KuduColumnHandle>) : RecordCursor {
    data class SizedEntry(val size: Long, val result: RowResult)

    var numRows: Long = 0
    var completed: Long = 0
    var current: RowResult? = null
    var iter: RowResultIterator? = null
    var rowResultIteratorNext: Deferred<RowResultIterator>? = if (scanner.hasMoreRows()) scanner.nextRows() else null
    var timeNano: Long = 0L

    override fun advanceNextPosition(): Boolean {
        if ((iter == null || !iter!!.hasNext()) && rowResultIteratorNext != null) {
            completed = 0
            iter = rowResultIteratorNext!!.join()
            numRows = iter!!.numRows.toLong()
            if (scanner.hasMoreRows()){
                rowResultIteratorNext = scanner.nextRows()
            }
        }
        if (iter != null && iter!!.hasNext()) {
            current = iter!!.next()
            completed += 1
            return true
        }
        return false
    }

    override fun getTotalBytes(): Long {
        return numRows
    }

    override fun getCompletedBytes(): Long {
        return completed
    }

    override fun getReadTimeNanos(): Long {
        return timeNano
    }

    override fun getType(field: Int): Type? {
        return columns[field].column.type.prestoType()
    }

    override fun getBoolean(field: Int): Boolean {
        return current!!.getBoolean(columns[field].column.name)
    }

    override fun getLong(field: Int): Long {
        val column = columns[field].column
        val result = current!!
        when (column.type) {
            null -> throw RuntimeException("Column must not be null")
            INT8, INT16, INT32 -> return result.getInt(column.name).toLong()
            INT64 -> return result.getLong(column.name)
            else -> throw RuntimeException("Type error")
        }
    }

    override fun getDouble(field: Int): Double {
        val column = columns[field].column
        val result = current!!
        when (column.type) {
            null -> throw RuntimeException("Column must not be null")
            FLOAT -> return result.getFloat(column.name).toDouble()
            DOUBLE -> return result.getDouble(column.name)
            else -> throw RuntimeException("Type error")
        }
    }

    override fun getSlice(field: Int): Slice? {
        val column = columns[field].column
        val result = current!!
        when (column.type) {
            BINARY -> return Slices.wrappedBuffer(result.getBinary(column.name))
            STRING -> return Slices.utf8Slice(result.getString(column.name))
            else -> throw RuntimeException("Type error")
        }
    }

    override fun getObject(field: Int): Any? {
        throw UnsupportedOperationException()
    }

    override fun isNull(field: Int): Boolean {
        val column = columns[field].column
        if (column.isNullable) {
            return current!!.isNull(column.name)
        }
        return false
    }

    override fun close() {
        scanner.close()
    }

}

class KuduRecordSet(val tabletId: ByteArray,
                    val layout: KuduTableLayoutHandle,
                    val columns: MutableList<out ColumnHandle>) : RecordSet {
    override fun getColumnTypes(): MutableList<Type> {
        return ImmutableList.copyOf(columns.map {
            if (it !is KuduColumnHandle) throw RuntimeException("Unknown column handle type")
            it.column.type.prestoType()
        })
    }

    override fun cursor(): RecordCursor? {
        val tablets = layout.query.tableHandle.table.getTabletsLocations(1000)
        val tablet = tablets.find {
            val res = false
            if (it.tabletId.size != tabletId.size) false
            else {
                var result = true
                for (i in 0..tabletId.size - 1) {
                    if (it.tabletId[i] != tabletId[i]) {
                        result = false
                        break
                    }
                }
                result
            }
        } ?: throw RuntimeException("Invalid tablet id")
        val scanner = layout.query.create(tablet.partition.rangeKeyStart, tablet.partition.rangeKeyEnd)
        val builder = ImmutableList.builder<KuduColumnHandle>()
        columns.forEach {
            if (it !is KuduColumnHandle) throw RuntimeException("Unknown column handle")
            builder.add(it)
        }
        if (scanner.hasMoreRows()) {
            return KuduRecordCursor(scanner, builder.build())
        } else {
            return null
        }
    }
}

class KuduRecordSetProvider : ConnectorRecordSetProvider {
    override fun getRecordSet(transactionHandle: ConnectorTransactionHandle?,
                              session: ConnectorSession?,
                              split: ConnectorSplit?,
                              columns: MutableList<out ColumnHandle>): RecordSet? {
        if (split !is KuduSplit) throw RuntimeException("Unknown split")
        return KuduRecordSet(split.tablet, split.layout, columns)
    }
}

class KuduSplitSource(val tablets: List<ByteArray>, val layout: KuduTableLayoutHandle) : ConnectorSplitSource {
    var pos = 0
    var done = false
    override fun getDataSourceName(): String? {
        return "kudu"
    }

    override fun getNextBatch(maxSize: Int): CompletableFuture<MutableList<ConnectorSplit>>? {
        val builder = ImmutableList.builder<ConnectorSplit>()
        for (i in pos..Math.min(pos + maxSize, tablets.size - 1)) {
            builder.add(KuduSplit(tablets[i], layout))
        }
        done = true
        return CompletableFuture.completedFuture(builder.build())
    }

    override fun close() {
    }

    override fun isFinished(): Boolean {
        return done
    }
}

class KuduSplitManager : ConnectorSplitManager {
    override fun getSplits(transactionHandle: ConnectorTransactionHandle?,
                           session: ConnectorSession?,
                           layout: ConnectorTableLayoutHandle?): ConnectorSplitSource? {
        if (layout !is KuduTableLayoutHandle) throw RuntimeException("Unknown table layout")
        val table = layout.query.tableHandle.table
        val tablets = table.getTabletsLocations(1000)
        return KuduSplitSource(ImmutableList.copyOf(tablets.map {
            it.tabletId
        }), layout)
    }
}

class KuduSplit : ConnectorSplit {
    @get:JsonGetter
    val tablet: ByteArray
    @get:JsonGetter
    val layout: KuduTableLayoutHandle

    @JsonCreator
    constructor(@JsonProperty("tablet") tablet: ByteArray,
                @JsonProperty("layout") layout: KuduTableLayoutHandle)
    {
        this.tablet = tablet
        this.layout = layout
    }

    override fun isRemotelyAccessible(): Boolean {
        return true
    }

    override fun getAddresses(): MutableList<HostAddress>? {
        return ImmutableList.of()
    }

    override fun getInfo(): Any? {
        return ""
    }
}
