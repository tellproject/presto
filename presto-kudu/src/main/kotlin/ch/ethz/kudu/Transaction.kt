package ch.ethz.kudu

import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider
import com.facebook.presto.spi.connector.ConnectorSplitManager
import com.facebook.presto.spi.connector.ConnectorTransactionHandle
import com.facebook.presto.spi.type.BigintType
import com.facebook.presto.spi.type.BooleanType
import com.facebook.presto.spi.type.Type
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.stumbleupon.async.Deferred
import io.airlift.slice.Slice
import io.airlift.slice.Slices
import org.kududb.Type.*
import org.kududb.client.*
import java.util.concurrent.CompletableFuture

class KuduTransactionHandle(val client: AsyncKuduClient, val session: AsyncKuduSession) : ConnectorTransactionHandle

class KuduRecordCursor(val scanner: AsyncKuduScanner, val columns: MutableList<out KuduColumnHandle>) : RecordCursor {
    data class SizedEntry(val size: Long, val result: RowResult)

    var current: MutableList<SizedEntry> = ImmutableList.of()
    var size: Long = -1
    var completed: Long = 0
    var idx = -1
    var rowResultIteratorNext: Deferred<RowResultIterator>? = if (scanner.hasMoreRows()) scanner.nextRows() else null
    var timeNano: Long = 0L

    override fun advanceNextPosition(): Boolean {
        if (!current.isEmpty() && current.size > ++idx) {
            completed += current[idx].size
            return true
        }
        if (rowResultIteratorNext != null) {
            val begin = System.nanoTime()
            val iter = rowResultIteratorNext!!.join()
            timeNano += System.nanoTime() - begin
            rowResultIteratorNext = null
            if (scanner.hasMoreRows()) {
                rowResultIteratorNext = scanner.nextRows()
            }
            if (iter.hasNext()) {
                size = 0
                val builder = ImmutableList.builder<SizedEntry>()
                iter.map {
                    val len = it.rowToString().length.toLong()
                    builder.add(SizedEntry(len, it))
                    size += len
                }
                current = builder.build()
                idx = 0
                completed = current[idx].size
                return true
            }
        }
        current = ImmutableList.of()
        idx = -1
        return false
    }

    override fun getTotalBytes(): Long {
        return size
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
        return current[idx].result.getBoolean(columns[field].column.name)
    }

    override fun getLong(field: Int): Long {
        val column = columns[field].column
        val result = current[idx].result
        when (column.type) {
            null -> throw RuntimeException("Column must not be null")
            INT8, INT16, INT32 -> return result.getInt(column.name).toLong()
            INT64 -> return result.getLong(column.name)
            else -> throw RuntimeException("Type error")
        }
    }

    override fun getDouble(field: Int): Double {
        val column = columns[field].column
        val result = current[idx].result
        when (column.type) {
            null -> throw RuntimeException("Column must not be null")
            FLOAT -> return result.getFloat(column.name).toDouble()
            DOUBLE -> return result.getDouble(column.name)
            else -> throw RuntimeException("Type error")
        }
    }

    override fun getSlice(field: Int): Slice? {
        val column = columns[field].column
        val result = current[idx].result
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
            return current[idx].result.isNull(column.name)
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
            it.tabletId == tabletId
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
    override fun getDataSourceName(): String? {
        return "kudu"
    }

    override fun getNextBatch(maxSize: Int): CompletableFuture<MutableList<ConnectorSplit>>? {
        val builder = ImmutableList.builder<ConnectorSplit>()
        for (i in pos..Math.min(pos + maxSize, tablets.size)) {
            builder.add(KuduSplit(tablets[i], layout))
        }
        return CompletableFuture.completedFuture(builder.build())
    }

    override fun close() {
    }

    override fun isFinished(): Boolean {
        return true
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

class KuduSplit(val tablet: ByteArray, val layout: KuduTableLayoutHandle) : ConnectorSplit {
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
