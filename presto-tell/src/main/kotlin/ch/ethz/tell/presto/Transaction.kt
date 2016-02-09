package ch.ethz.tell.presto

import ch.ethz.tell.*
import ch.ethz.tell.Field.FieldType.*
import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider
import com.facebook.presto.spi.connector.ConnectorSplitManager
import com.facebook.presto.spi.connector.ConnectorTransactionHandle
import com.facebook.presto.spi.predicate.Domain
import com.facebook.presto.spi.predicate.EquatableValueSet
import com.facebook.presto.spi.predicate.SortedRangeSet
import com.facebook.presto.spi.type.Type
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.collect.ImmutableList
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

    @JsonProperty
    fun getTransactionId(): Long {
        return transaction.transactionId
    }
}

fun toPredicateType(type: Field.FieldType, obj: Any): PredicateType {
    return when (type) {
        SMALLINT -> PredicateType.create((obj as Long).toShort())
        INT -> PredicateType.create((obj as Long).toInt())
        BIGINT -> PredicateType.create(obj as Long)
        FLOAT -> PredicateType.create((obj as Double).toFloat())
        DOUBLE -> PredicateType.create(obj as Double)
        TEXT -> PredicateType.create(obj as String)
        BLOB -> PredicateType.create(obj as ByteArray)
        NOTYPE, NULLTYPE -> throw RuntimeException("Unknown type")
    }
}

data class FieldMetadata(val fieldType: Field.FieldType, val idx: Short, val notNull: Boolean)

class TellRecordSet(val scanMemoryManager: ScanMemoryManager,
                    val transaction: Transaction,
                    val split: TellSplit,
                    val columns: List<TellColumnHandle>) : RecordSet {

    override fun getColumnTypes(): MutableList<Type>? {
        return columns.map {
            it.field.prestoType()
        }.toMutableList()
    }

    override fun cursor(): RecordCursor? {
        val query = ScanQuery(
                split.layout.table.table.tableName,
                split.partitionShift,
                split.splitNum,
                split.numSplits)
        // add projections
        columns.forEach {
            query.addProjection(it.field.index, it.field.fieldName, it.field.fieldType, it.field.notNull)
        }
        split.layout.domain.domains.ifPresent {
            // TODO: calculate unenforcedConstraint - move this code to MetData.kt (building of CNFs)
            it.forEach {
                val clause = CNFClause()
                var didAdd = false
                val field = (it.key as? TellColumnHandle)?.field ?: throw RuntimeException("Unknown field type")
                if (it.value.isOnlyNull) {
                    clause.addPredicate(ScanQuery.CmpType.IS_NULL, field.index, null)
                } else if (it.value == Domain.notNull(it.value.type)) {
                    clause.addPredicate(ScanQuery.CmpType.IS_NOT_NULL, field.index, null)
                } else if (it.value.isSingleValue) {
                    clause.addPredicate(ScanQuery.CmpType.EQUAL, field.index,
                            toPredicateType(field.fieldType, it.value.values.singleValue))
                } else if (it.value.values is SortedRangeSet) {
                    didAdd = true
                    val ranges = it.value.values.ranges
                    if (ranges.rangeCount > 4) throw RuntimeException("Unsupported number of ranges")
                    for (i in 0..ranges.rangeCount - 1) {
                        val low = ranges.orderedRanges[i].low
                        for (j in i..ranges.rangeCount - 1) {
                            val jlow = ranges.orderedRanges[j].low
                            val jhigh = ranges.orderedRanges[j].high

                            var cnf = CNFClause()
                            cnf.addPredicate(ScanQuery.CmpType.GREATER, field.index, toPredicateType(field.fieldType, low.value))
                            cnf.addPredicate(ScanQuery.CmpType.GREATER, field.index, toPredicateType(field.fieldType, jlow.value))
                            query.addSelection(cnf)

                            cnf = CNFClause()
                            cnf.addPredicate(ScanQuery.CmpType.GREATER, field.index, toPredicateType(field.fieldType, low.value))
                            cnf.addPredicate(ScanQuery.CmpType.LESS, field.index, toPredicateType(field.fieldType, jhigh.value))
                            query.addSelection(cnf)
                        }

                        val high = ranges.orderedRanges[i].high
                        for (j in i..ranges.rangeCount - 1) {
                            val jlow = ranges.orderedRanges[j].low
                            val jhigh = ranges.orderedRanges[j].high

                            var cnf = CNFClause()
                            cnf.addPredicate(ScanQuery.CmpType.LESS, field.index, toPredicateType(field.fieldType, high.value))
                            cnf.addPredicate(ScanQuery.CmpType.GREATER, field.index, toPredicateType(field.fieldType, jlow.value))
                            query.addSelection(cnf)

                            cnf = CNFClause()
                            cnf.addPredicate(ScanQuery.CmpType.LESS, field.index, toPredicateType(field.fieldType, high.value))
                            cnf.addPredicate(ScanQuery.CmpType.LESS, field.index, toPredicateType(field.fieldType, jhigh.value))
                            query.addSelection(cnf)
                        }
                    }
                } else if (it.value.values is EquatableValueSet) {
                    var values = it.value.values.discreteValues
                    if (values.values.size > 20) throw RuntimeException("Can not support so many distinct values")
                    if (values.isWhiteList) {
                        val cnf = CNFClause()
                        values.values.forEach {
                            cnf.addPredicate(ScanQuery.CmpType.EQUAL, field.index, toPredicateType(field.fieldType, it))
                        }
                        query.addSelection(cnf)
                    } else {
                        values.values.forEach {
                            val cnf = CNFClause()
                            cnf.addPredicate(ScanQuery.CmpType.NOT_EQUAL, field.index, toPredicateType(field.fieldType, it))
                            query.addSelection(cnf)
                        }
                    }
                } else {
                    throw RuntimeException("Unsupported domain")
                }
                if (!didAdd) query.addSelection(clause)
            }
        }
        val querySchema = query.resultSchema
        val scanResult = transaction.scan(scanMemoryManager, query)
        return object : RecordCursor {
            var finished = false
            var chunkPos = 0L
            var chunkEnd = 0L
            var record = 0L
            var timer = 0L
            val fieldMeta = querySchema.fieldNames.map {
                val field = querySchema.getFieldByName(it)
                FieldMetadata(field.fieldType, field.index, field.notNull)
            }

            val unsafe: sun.misc.Unsafe = Unsafe.getUnsafe()

            val positions = Array(fieldMeta.size, { 0.toLong() })
            val posPos = columns.map {
                var res = -1
                for (i in 0..fieldMeta.size) {
                    if (fieldMeta[i].idx == it.field.index) {
                        res = i
                    }
                }
                res
            }

            fun next() {
                if (!hasNext()) {
                    throw NoSuchElementException("End of stream")
                }

                // Skip key
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

                var nullIdx = 0
                for (i in 0..fieldMeta.size - 1) {
                    val field = fieldMeta[i]
                    positions[i] = chunkPos
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
                return columns[field].field.prestoType()
            }

            override fun getBoolean(field: Int): Boolean {
                val pos = positions[posPos[field]]
                return unsafe.getShort(pos) != 0.toShort()
            }

            override fun getLong(field: Int): Long {
                val pos = positions[posPos[field]]
                when (fieldMeta[posPos[field]].fieldType) {
                    SMALLINT -> return unsafe.getShort(pos).toLong()
                    INT -> return unsafe.getInt(pos).toLong()
                    BIGINT -> return unsafe.getLong(pos)
                    else -> throw RuntimeException("Unexpected type: ${fieldMeta[posPos[field]].fieldType}")
                }
            }

            override fun getDouble(field: Int): Double {
                val pos = positions[posPos[field]]
                when (fieldMeta[posPos[field]].fieldType) {
                    FLOAT -> return unsafe.getFloat(pos).toDouble()
                    DOUBLE -> return unsafe.getDouble(pos)
                    else -> throw RuntimeException("Unexpected type: ${fieldMeta[posPos[field]].fieldType}")
                }
            }

            override fun getSlice(field: Int): Slice? {
                when (fieldMeta[posPos[field]].fieldType) {
                    BLOB, TEXT -> {}
                    else -> throw RuntimeException("Unexcpected operation")
                }
                val pos = positions[posPos[field]]
                val offset = unsafe.getInt(pos)
                val length = unsafe.getInt(pos + 4) - offset
                val value = ByteArray(length, {
                    unsafe.getByte(pos + it)
                })
                when (fieldMeta[posPos[field]].fieldType) {
                    TEXT -> return Slices.utf8Slice(value.toString(Charset.forName("UTF-8")))
                    BLOB -> return Slices.wrappedBuffer(value, 0, value.size)
                    else -> throw RuntimeException("Unexpected type: ${fieldMeta[posPos[field]].fieldType}")
                }
            }

            override fun getObject(field: Int): Any? {
                throw UnsupportedOperationException()
            }

            override fun isNull(field: Int): Boolean {
                val f = fieldMeta[posPos[field]]
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
            }

        }
    }

}

class TellRecordSetProvider(val scanMemoryManager: ScanMemoryManager,
                            val clientManager: ClientManager) : ConnectorRecordSetProvider {
    val log = LogFactory.getLog(TellRecordSetProvider::class.java)

    override fun getRecordSet(transactionHandle: ConnectorTransactionHandle?,
                              session: ConnectorSession?,
                              split: ConnectorSplit?,
                              columns: MutableList<out ColumnHandle>?): RecordSet? {
        log.info("passed transaction: ${(transactionHandle as TellTransactionHandle).transaction.transactionId}")
        if (split !is TellSplit) throw RuntimeException("Unknown split")
        if (transactionHandle !is TellTransactionHandle) throw RuntimeException("Unknown transaction handle")
        val columnTypes = columns?.map {
            if (it !is TellColumnHandle) throw RuntimeException("Unsupported column handle")
            it.field.prestoType()
        } ?: throw RuntimeException("no Columns given")
        return TellRecordSet(scanMemoryManager, transactionHandle.transaction, split, columns?.map {
            it as? TellColumnHandle ?: throw RuntimeException("Unknown column handle") } ?: throw RuntimeException(""))
    }
}

class TellSplitSource(val layout: TellTableLayoutHandle,
                      val transactionId: Long,
                      val numPartitions: Int,
                      val partitionShift: Int) : ConnectorSplitSource {
    val log = LogFactory.getLog(TellSplitSource::class.java)
    var finished = false

    override fun getDataSourceName(): String? {
        return "tell"
    }

    override fun getNextBatch(maxSize: Int): CompletableFuture<MutableList<ConnectorSplit>>? {
        finished = true
        val partitions = Math.min(maxSize, numPartitions)
        val result = ImmutableList.builder<ConnectorSplit>()
        for (i in 1..partitions) result.add(TellSplit(layout, transactionId, i - 1, numPartitions, partitionShift))
        return CompletableFuture.completedFuture(result.build())
    }

    override fun close() {
        log.info("close")
    }

    override fun isFinished(): Boolean {
        return finished
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

    @JsonCreator
    constructor(@JsonProperty("layout") layout: TellTableLayoutHandle,
                @JsonProperty("transactionId") transactionId: Long,
                @JsonProperty("splitId") splitNum: Int,
                @JsonProperty("numSplits") numSplits: Int,
                @JsonProperty("partitionShift") partitionShift: Int) {
        this.layout = layout
        this.transactionId = transactionId
        this.splitNum = splitNum
        this.numSplits = numSplits
        this.partitionShift = partitionShift
    }

    val log = LogFactory.getLog(TellSplit::class.java)

    override fun isRemotelyAccessible(): Boolean {
        return true
    }

    override fun getAddresses(): MutableList<HostAddress>? {
        return ImmutableList.of<HostAddress>()
    }

    override fun getInfo(): Any? {
        log.info("getInfo -> $transactionId")
        return transactionId
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
        return TellSplitSource(layout, transactionHandle.transaction.transactionId, numPartitions, partitionShift)
    }
}
