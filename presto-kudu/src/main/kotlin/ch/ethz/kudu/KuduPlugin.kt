package ch.ethz.kudu

import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.*
import com.facebook.presto.spi.connector.Connector
import com.facebook.presto.spi.connector.ConnectorFactory
import com.facebook.presto.spi.connector.ConnectorMetadata
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider
import com.facebook.presto.spi.connector.ConnectorSplitManager
import com.facebook.presto.spi.transaction.IsolationLevel
import com.google.common.collect.ImmutableList
import org.kududb.client.AsyncKuduClient

class KuduHandleResolver : ConnectorHandleResolver {
    override fun getTableHandleClass(): Class<out ConnectorTableHandle>? {
        return KuduTableHandle::class.java
    }

    override fun getTableLayoutHandleClass(): Class<out ConnectorTableLayoutHandle>? {
        return KuduTableLayoutHandle::class.java
    }

    override fun getColumnHandleClass(): Class<out ColumnHandle>? {
        return KuduColumnHandle::class.java
    }

    override fun getSplitClass(): Class<out ConnectorSplit>? {
        return KuduSplit::class.java
    }

    override fun getTransactionHandleClass(): Class<out ConnectorTransactionHandle>? {
        return KuduTransactionHandle::class.java
    }
}

object ClientSingleton {
    var client: AsyncKuduClient? = null

    fun client(master: String): AsyncKuduClient {
        if (client == null) {
            synchronized(this) {
                if (client == null) {
                    client = AsyncKuduClient.AsyncKuduClientBuilder(master).build()
                }
            }
        }
        return client!!
    }
}

class KuduConnector(val client: AsyncKuduClient) : Connector {
    override fun beginTransaction(isolationLevel: IsolationLevel?, readOnly: Boolean): ConnectorTransactionHandle? {
        return KuduTransactionHandle()
    }

    override fun getMetadata(transactionHandle: ConnectorTransactionHandle?): ConnectorMetadata? {
        if (transactionHandle !is KuduTransactionHandle) throw RuntimeException("unknown transaction handle")
        return KuduMetadata(transactionHandle)
    }

    override fun getSplitManager(): ConnectorSplitManager? {
        return KuduSplitManager()
    }

    override fun getRecordSetProvider(): ConnectorRecordSetProvider? {
        return KuduRecordSetProvider()
    }
}

class KuduConnectionFactory : ConnectorFactory {
    override fun getName(): String? {
        return "kudu"
    }

    override fun getHandleResolver(): ConnectorHandleResolver? {
        return KuduHandleResolver()
    }

    override fun create(connectorId: String?, config: MutableMap<String, String>): Connector? {
        return KuduConnector(ClientSingleton.client(config["kudu.master"] ?: throw RuntimeException("kudu.master not set")))
    }

}

class KuduPlugin : Plugin {
    override fun <T : Any?> getServices(type: Class<T>?): MutableList<T>? {
        if (type == ConnectorFactory::class.java) {
            return ImmutableList.of(type.cast(KuduConnectionFactory()));
        }
        return ImmutableList.of();
    }
}
