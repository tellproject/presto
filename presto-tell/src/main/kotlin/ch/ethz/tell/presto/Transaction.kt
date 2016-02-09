package ch.ethz.tell.presto

import ch.ethz.tell.*
import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider
import com.facebook.presto.spi.connector.ConnectorSplitManager
import com.facebook.presto.spi.connector.ConnectorTransactionHandle
import com.facebook.presto.spi.type.Type
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.collect.ImmutableList
import org.apache.commons.logging.LogFactory
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
            it.forEach {
                val clause = CNFClause()
            }
        }
        throw UnsupportedOperationException()
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
