package kudu.ch.ethz.kudu

import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.ConnectorSplitManager
import com.facebook.presto.spi.connector.ConnectorTransactionHandle
import org.kududb.client.KuduClient
import org.kududb.client.KuduSession

class KuduTransactionHandle(val client: KuduClient, val session: KuduSession) : ConnectorTransactionHandle

class KuduSplitManager : ConnectorSplitManager {
    override fun getSplits(transactionHandle: ConnectorTransactionHandle?,
                           session: ConnectorSession?,
                           layout: ConnectorTableLayoutHandle?): ConnectorSplitSource? {
        return super.getSplits(transactionHandle, session, layout)
    }
}

class KuduSplit : ConnectorSplit {
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
