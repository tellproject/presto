package ch.ethz.tell.presto

import ch.ethz.tell.*
import ch.ethz.tell.Field.FieldType.*
import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider
import com.facebook.presto.spi.connector.ConnectorSplitManager
import com.facebook.presto.spi.connector.ConnectorTransactionHandle
import com.facebook.presto.spi.type.BigintType
import com.facebook.presto.spi.type.Type
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import io.airlift.slice.Slice
import io.airlift.slice.Slices
import org.apache.commons.logging.LogFactory
import java.nio.charset.Charset
import java.util.*
import java.util.concurrent.CompletableFuture

class TellTransactionHandle(val transaction: Transaction) : ConnectorTransactionHandle {
    @JsonCreator
    constructor(@JsonProperty("transactionId") transactionId: Long) : this(
            Transaction.startTransaction(transactionId, TellConnection.clientManager))

    @get:JsonProperty
    val transactionId: Long
        get() {
            return transaction.transactionId
        }
}

fun toPredicateType(type: Field.FieldType, value: Any?): PredicateType {
    val obj: Any = value ?: when (type) {
        SMALLINT -> 1
        INT -> 1
        BIGINT -> 1
        FLOAT -> 1.0
        DOUBLE -> 1.0
        TEXT -> ""
        BLOB -> ""
        NOTYPE, NULLTYPE -> throw RuntimeException("Unknown type")
    }
    return when (type) {
        SMALLINT -> PredicateType.create((obj as Number).toShort())
        INT -> PredicateType.create((obj as Number).toInt())
        BIGINT -> PredicateType.create((obj as Number).toLong())
        FLOAT -> PredicateType.create((obj as Number).toFloat())
        DOUBLE -> PredicateType.create((obj as Number).toLong())
        TEXT -> PredicateType.create(obj as String)
        BLOB -> PredicateType.create(obj as ByteArray)
        NOTYPE, NULLTYPE -> throw RuntimeException("Unknown type")
    }
}

data class FieldMetadata(val fieldType: Field.FieldType, val idx: Short, val notNull: Boolean, val name: String)

class TellRecordCursor(val transaction: Transaction,
                       val scanMemoryManager: ScanMemoryManager,
                       val query: ScanQuery,
                       val querySchema: Schema,
                       val columns: MutableList<out ColumnHandle>) : RecordCursor {
    val scanResult = transaction.scan(scanMemoryManager, query)
    var finished = false
    var chunkPos = 0L
    var chunkEnd = 0L
    var record = 0L
    var timer = 0L
    val fieldMeta = querySchema.fieldNames.map {
        val field = querySchema.getFieldByName(it)
        FieldMetadata(field.fieldType, field.index, field.notNull, field.fieldName)
    }

    var counter = 0

    val log = LogFactory.getLog(TellRecordCursor::class.java)

    val unsafe = Unsafe.getUnsafe()

    val positions = Array(fieldMeta.size + 1, { 0.toLong() })
    val posPos = columns.map {
        if (it !is TellColumnHandle) throw RuntimeException("Unknown column")
        var res = -1
        if (it.field == null) {
            0
        } else {
            for (i in 0..fieldMeta.size - 1) {
                if (fieldMeta[i].name == it.field.fieldName) {
                    res = i + 1
                }
            }
            assert(res != -1)
            res
        }
    }

    fun next() {
        ++counter
        if (!hasNext()) {
            throw NoSuchElementException("End of stream")
        }

        // key
        positions[0] = chunkPos
        chunkPos += 8

        // Skip header
        record = chunkPos
        chunkPos += querySchema.headerLength

        // Align for fields
        if (chunkPos % 8 != 0.toLong()) chunkPos += 8 - chunkPos % 8

        var hasVariable = false
        // The size of the variable heap
        // Initialized to 4 to skip the additional offset field at the end
        var variableLength = 4

        for (i in 0..fieldMeta.size - 1) {
            val field = fieldMeta[i]
            positions[i+1] = chunkPos
            when (field.fieldType) {
                SMALLINT -> chunkPos += 2
                INT, FLOAT -> chunkPos += 4
                BIGINT, DOUBLE ->
                    chunkPos += 8

                TEXT, BLOB -> {
                    if (!hasVariable) {
                        hasVariable = true

                        // Align to 4 for the first variable size offset
                        if (chunkPos % 4.toLong() != 0.toLong()) chunkPos += 4 - chunkPos % 4
                    }

                    val offset = unsafe.getInt(chunkPos)
                    val length = unsafe.getInt(chunkPos + 4) - offset
                    variableLength += length
                    chunkPos += 4
                }

                else -> throw RuntimeException("Unsupported type ${field.fieldType}")
            }
        }
        // Skip the heap if the record has any variable size fields
        if (hasVariable) {
            chunkPos += variableLength
        }

        // Align to next record
        if (chunkPos % 8 != 0.toLong()) chunkPos += 8 - chunkPos % 8
    }

    fun hasNext(): Boolean {
        if (!finished && chunkPos == chunkEnd) {
            val begin = System.nanoTime()
            val hasChunk = scanResult.next()
            timer += (System.nanoTime() - begin)
            if (hasChunk) {
                chunkPos = scanResult.address()
                chunkEnd = chunkPos + scanResult.length()
            } else {
                transaction.commit()
                finished = true
            }
        }
        return !finished
    }

    override fun advanceNextPosition(): Boolean {
        if (hasNext()) {
            next()
            return true
        }
        return false
    }

    override fun getTotalBytes(): Long {
        return chunkEnd - record
    }

    override fun getCompletedBytes(): Long {
        return scanResult.length()
    }

    override fun getReadTimeNanos(): Long {
        return timer
    }

    override fun getType(field: Int): Type? {
        val handle = columns[field]
        if (handle !is TellColumnHandle) throw RuntimeException("Unknown column hanle")
        if (handle.field == null)
            return BigintType.BIGINT
        else
            return handle.field.prestoType()
    }

    override fun getBoolean(field: Int): Boolean {
        val pos = positions[posPos[field]]
        return unsafe.getShort(pos) != 0.toShort()
    }

    override fun getLong(field: Int): Long {
        val pos = positions[posPos[field]]
        if (posPos[field] == 0) {
            return unsafe.getLong(positions[0])
        }
        when (fieldMeta[posPos[field] - 1].fieldType) {
            SMALLINT -> return unsafe.getShort(pos).toLong()
            INT -> return unsafe.getInt(pos).toLong()
            BIGINT -> return unsafe.getLong(pos)
            else -> throw RuntimeException("Unexpected type: ${fieldMeta[posPos[field]].fieldType}")
        }
    }

    override fun getDouble(field: Int): Double {
        val pos = positions[posPos[field]]
        when (fieldMeta[posPos[field] - 1].fieldType) {
            FLOAT -> return unsafe.getFloat(pos).toDouble()
            DOUBLE -> return unsafe.getDouble(pos)
            else -> throw RuntimeException("Unexpected type: ${fieldMeta[posPos[field]].fieldType}")
        }
    }

    override fun getSlice(field: Int): Slice? {
        when (fieldMeta[posPos[field] - 1].fieldType) {
            BLOB, TEXT -> {}
            else -> throw RuntimeException("Unexcpected operation")
        }
        val pos = positions[posPos[field]]
        val offset = unsafe.getInt(pos)
        val length = unsafe.getInt(pos + 4) - offset
        val strPos = record + offset
        val value = ByteArray(length, {
            unsafe.getByte(strPos + it)
        })
        when (fieldMeta[posPos[field] - 1].fieldType) {
            TEXT -> return Slices.utf8Slice(value.toString(Charset.forName("UTF-8")))
            BLOB -> return Slices.wrappedBuffer(value, 0, value.size)
            else -> throw RuntimeException("Unexpected type: ${fieldMeta[posPos[field]].fieldType}")
        }
    }

    override fun getObject(field: Int): Any? {
        throw UnsupportedOperationException()
    }

    override fun isNull(field: Int): Boolean {
        if (posPos[field] == 0) return false
        val f = fieldMeta[posPos[field] - 1]
        if (!f.notNull) {
            var idx = 0
            for (fM in fieldMeta) {
                if (fM.idx == f.idx) {
                    break
                }
                ++idx
            }
            return unsafe.getByte(record + idx).toInt() != 0
        }
        return false
    }

    override fun close() {
        while (hasNext()) next()
        log.warn("Finished scanning Split ${query.partitionValue} of ${query.partitionKey} - scanned $counter tuples")
    }
}

class TellRecordSet(val scanMemoryManager: ScanMemoryManager,
                    val transaction: Transaction,
                    val split: TellSplit,
                    val columns: MutableList<out ColumnHandle>) : RecordSet {

    val log = LogFactory.getLog(TellRecordSet::class.java)

    override fun getColumnTypes(): MutableList<Type>? {
        return columns.map {
            if (it !is TellColumnHandle) throw RuntimeException("Unknown column handle")
            if (it.field == null)
                BigintType.BIGINT
            else
                it.field.prestoType()
        }.toMutableList()
    }

    override fun cursor(): RecordCursor? {
        val q = split.layout.scanQuery
        val query =
                if (split.numSplits == 1)
                    q.create(0, 0, 0)
                else q.create(split.partitionShift, split.splitNum, split.numSplits)
        log.warn("Start scan: shift: ${split.partitionShift} partition: ${split.splitNum} partitions: ${split.numSplits}")
        val hasProjections = q.queryType == ScanQuery.QueryType.PROJECTION
        val querySchema = if (hasProjections) query.resultSchema else split.layout.scanQuery.table.schema
        log.warn("Starting scan")
        return TellRecordCursor(transaction, scanMemoryManager, query, querySchema, columns)
    }

}

class TellRecordSetProvider(val scanMemoryManager: ScanMemoryManager) : ConnectorRecordSetProvider {
    val log = LogFactory.getLog(TellRecordSetProvider::class.java)

    override fun getRecordSet(transactionHandle: ConnectorTransactionHandle?,
                              session: ConnectorSession?,
                              split: ConnectorSplit?,
                              columns: MutableList<out ColumnHandle>): RecordSet? {
        log.info("passed transaction: ${(transactionHandle as TellTransactionHandle).transaction.transactionId}")
        if (split !is TellSplit) throw RuntimeException("Unknown split")
        if (transactionHandle !is TellTransactionHandle) throw RuntimeException("Unknown transaction handle")
        return TellRecordSet(scanMemoryManager, transactionHandle.transaction, split, columns)
    }
}

class TellSplit : ConnectorSplit {
    @get:JsonGetter
    val layout: TellTableLayoutHandle
    @get:JsonGetter
    val transactionId: Long
    @get:JsonGetter
    val splitNum: Int
    @get:JsonGetter
    val numSplits: Int
    @get:JsonGetter
    val partitionShift: Int

    val log = LogFactory.getLog(TellSplit::class.java)

    @JsonCreator
    constructor(@JsonProperty("layout") layout: TellTableLayoutHandle,
                @JsonProperty("transactionId") transactionId: Long,
                @JsonProperty("splitNum") splitNum: Int,
                @JsonProperty("numSplits") numSplits: Int,
                @JsonProperty("partitionShift") partitionShift: Int) {
        log.warn("deserialized/created split: {table=${layout.scanQuery.table.tableName}, split=$splitNum, totalSplit=$numSplits}")
        this.layout = layout
        this.transactionId = transactionId
        this.splitNum = splitNum
        this.numSplits = numSplits
        this.partitionShift = partitionShift
    }

    override fun isRemotelyAccessible(): Boolean {
        return true
    }

    override fun getAddresses(): MutableList<HostAddress>? {
        return ImmutableList.of<HostAddress>()
    }

    override fun toString(): String {
        return "{table=${layout.scanQuery.table.tableName}, split=$splitNum, totalSplit=$numSplits}"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is TellSplit) return false
        return other.layout.scanQuery.table.tableId == layout.scanQuery.table.tableId &&
                other.splitNum == splitNum && other.layout.scanQuery == layout.scanQuery
    }

    override fun hashCode(): Int {
        return toString().hashCode()
    }

    override fun getInfo(): Any? {
        return ImmutableMap.builder<String, Any>()
                .put("table", layout.scanQuery.table.tableName)
                .put("tableId", layout.scanQuery.table.tableId)
                .put("partitionId", splitNum)
                .build();
    }
}

class TellSplitManager(val numPartitions: Int, val partitionShift: Int) : ConnectorSplitManager {
    override fun getSplits(transactionHandle: ConnectorTransactionHandle?,
                           session: ConnectorSession?,
                           layout: ConnectorTableLayoutHandle?): ConnectorSplitSource? {
        if (transactionHandle !is TellTransactionHandle) {
            throw RuntimeException("wrong transaction handle")
        }
        if (layout !is TellTableLayoutHandle) throw RuntimeException("Wrong layout")
        if (transactionHandle !is TellTransactionHandle) throw RuntimeException("Unknown transaction handle")
        val splits = Array(numPartitions, {
            TellSplit(layout, transactionHandle.transactionId, it, numPartitions, partitionShift)
        })
        return FixedSplitSource("tell", splits.toList())
        //return TellSplitSource(layout, transactionHandle.transaction.transactionId, numPartitions, partitionShift)
    }
}
