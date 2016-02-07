package ch.ethz.tell.presto

import ch.ethz.tell.ClientManager
import ch.ethz.tell.Transaction
import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.Connector
import com.facebook.presto.spi.connector.ConnectorFactory
import com.facebook.presto.spi.connector.ConnectorMetadata
import com.facebook.presto.spi.connector.ConnectorSplitManager
import com.facebook.presto.spi.connector.ConnectorTransactionHandle
import com.facebook.presto.spi.transaction.IsolationLevel
import com.google.common.collect.ImmutableList

class TellHandleResolver : ConnectorHandleResolver {
    override fun getTableHandleClass(): Class<out ConnectorTableHandle>? {
        throw UnsupportedOperationException()
    }

    override fun getTableLayoutHandleClass(): Class<out ConnectorTableLayoutHandle>? {
        throw UnsupportedOperationException()
    }

    override fun getColumnHandleClass(): Class<out ColumnHandle>? {
        throw UnsupportedOperationException()
    }

    override fun getSplitClass(): Class<out ConnectorSplit>? {
        throw UnsupportedOperationException()
    }
}

object ClientManagerSingleton {
    private var clientManager: ClientManager? = null

    fun clientManager(config: MutableMap<String, String>): ClientManager {
        if (clientManager == null) {
            synchronized(this) {
                if (clientManager == null) {
                    clientManager = ClientManager(config.get("tell.commitManager"), config.get("tell.storages"));
                }
            }
        }
        return clientManager!!
    }
}

class TellConnector(private val id: String, private val config: MutableMap<String, String>) : Connector {

    val clientManager: ClientManager = ClientManagerSingleton.clientManager(config)

    override fun beginTransaction(isolationLevel: IsolationLevel, readOnly: Boolean): ConnectorTransactionHandle? {
        if (!readOnly) return null;
        return TellTransactionHandle(Transaction.startTransaction(clientManager))
    }

    override fun getMetadata(transactionHandle: ConnectorTransactionHandle?): ConnectorMetadata? {
        if (transactionHandle is TellTransactionHandle) {
            return TellMetadata(transactionHandle.transaction)
        }
        throw RuntimeException("Wrong transaction handle type")
    }

    override fun getSplitManager(): ConnectorSplitManager? {
        throw UnsupportedOperationException()
    }
}

class TellConnection : ConnectorFactory {
    override fun getName(): String {
        return "tell"
    }

    override fun getHandleResolver(): ConnectorHandleResolver? {
        return TellHandleResolver()
    }

    override fun create(connectorId: String, config: MutableMap<String, String>): Connector {
        return TellConnector(connectorId, config)
    }
}

class TellPlugin : Plugin {
    override fun <T : Any?> getServices(type: Class<T>?): MutableList<T>? {
        if (type == ConnectorFactory::class.java) {
            return ImmutableList.of(type.cast(TellConnection()));
        }
        return ImmutableList.of();
    }
}