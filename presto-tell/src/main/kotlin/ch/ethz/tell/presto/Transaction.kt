package ch.ethz.tell.presto

import ch.ethz.tell.Transaction
import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider
import com.facebook.presto.spi.connector.ConnectorSplitManager
import com.facebook.presto.spi.connector.ConnectorTransactionHandle
import java.util.concurrent.CompletableFuture

class TellTransactionHandle(val transaction: Transaction) : ConnectorTransactionHandle

class TellRecordSetProvider : ConnectorRecordSetProvider {
    override fun getRecordSet(transactionHandle: ConnectorTransactionHandle?, session: ConnectorSession?, split: ConnectorSplit?, columns: MutableList<out ColumnHandle>?): RecordSet? {
        throw UnsupportedOperationException()
    }
}

class TellSplitSource : ConnectorSplitSource {
    override fun getDataSourceName(): String? {
        throw UnsupportedOperationException()
    }

    override fun getNextBatch(maxSize: Int): CompletableFuture<MutableList<ConnectorSplit>>? {
        throw UnsupportedOperationException()
    }

    override fun close() {
        throw UnsupportedOperationException()
    }

    override fun isFinished(): Boolean {
        throw UnsupportedOperationException()
    }
}

class TellSplit : ConnectorSplit {
    override fun isRemotelyAccessible(): Boolean {
        throw UnsupportedOperationException()
    }

    override fun getAddresses(): MutableList<HostAddress>? {
        throw UnsupportedOperationException()
    }

    override fun getInfo(): Any? {
        throw UnsupportedOperationException()
    }
}

class TellSplitManager : ConnectorSplitManager {
    override fun getSplits(transactionHandle: ConnectorTransactionHandle?, session: ConnectorSession?, layout: ConnectorTableLayoutHandle?): ConnectorSplitSource? {
        return super.getSplits(transactionHandle, session, layout)
    }
}
