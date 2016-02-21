package ch.ethz.tell.presto

import ch.ethz.tell.ClientManager
import ch.ethz.tell.ScanMemoryManager
import ch.ethz.tell.Transaction
import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.*
import com.facebook.presto.spi.connector.Connector
import com.facebook.presto.spi.connector.ConnectorFactory
import com.facebook.presto.spi.connector.ConnectorMetadata
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider
import com.facebook.presto.spi.connector.ConnectorSplitManager
import com.facebook.presto.spi.transaction.IsolationLevel
import com.facebook.presto.spi.type.Type
import com.google.common.collect.ImmutableList
import org.apache.commons.logging.LogFactory
import java.util.function.ToIntFunction

class TellHandleResolver : ConnectorHandleResolver {
    override fun getTableHandleClass(): Class<out ConnectorTableHandle>? {
        return TellTableHandle::class.java
    }

    override fun getTableLayoutHandleClass(): Class<out ConnectorTableLayoutHandle>? {
        return TellTableLayoutHandle::class.java
    }

    override fun getColumnHandleClass(): Class<out ColumnHandle>? {
        return TellColumnHandle::class.java
    }

    override fun getSplitClass(): Class<out ConnectorSplit>? {
        return TellSplit::class.java
    }

    override fun getTransactionHandleClass(): Class<out ConnectorTransactionHandle>? {
        return TellTransactionHandle::class.java
    }
}

object TellConnection {
    var clientManager: ClientManager? = null
    var scanMemoryManager: ScanMemoryManager? = null

    val log = LogFactory.getLog(TellConnection::class.java)

    fun clientManager(config: MutableMap<String, String>): ClientManager {
        if (clientManager == null) {
            synchronized(this) {
                if (clientManager == null) {
                    log.info("Initializing ClientManager")
                    clientManager = ClientManager(config["tell.commitManager"], config["tell.storages"]);
                }
            }
        }
        return clientManager!!
    }

    fun scanMemoryManager(config: MutableMap<String, String>): ScanMemoryManager {
        if (scanMemoryManager == null) {
            synchronized(this) {
                if (scanMemoryManager == null) {
                    log.info("Initialize ScanMemoryManager")
                    scanMemoryManager = ScanMemoryManager(
                            clientManager(config),
                            config["tell.chunkCount"]?.toLong() ?: throw RuntimeException("chunkCount not set"),
                            config["tell.chunkSize"]?.toLong() ?: throw RuntimeException("chunkSize not set"))
                    log.info("[DONE] Initialize ScanMemoryManager")
                }
            }
        }
        return scanMemoryManager!!
    }
}

class TellConnector(private val id: String, private val config: MutableMap<String, String>) : Connector {

    val clientManager: ClientManager = TellConnection.clientManager(config)

    override fun beginTransaction(isolationLevel: IsolationLevel, readOnly: Boolean): ConnectorTransactionHandle? {
        //if (!readOnly) return null;
        return TellTransactionHandle(Transaction.startTransaction(clientManager))
    }

    override fun commit(transactionHandle: ConnectorTransactionHandle?) {
        if (transactionHandle !is TellTransactionHandle) throw RuntimeException("Unkown transaction handle")
        transactionHandle.transaction.commit()
    }

    override fun rollback(transactionHandle: ConnectorTransactionHandle?) {
        if (transactionHandle !is TellTransactionHandle) throw RuntimeException("Unkown transaction handle")
        transactionHandle.transaction.abort()
    }

    override fun getMetadata(transactionHandle: ConnectorTransactionHandle?): ConnectorMetadata? {
        if (transactionHandle is TellTransactionHandle) {
            return TellMetadata(transactionHandle.transaction)
        }
        throw RuntimeException("Wrong transaction handle type")
    }

    override fun getSplitManager(): ConnectorSplitManager? {
        return TellSplitManager(
                config["tell.numPartitions"]?.toInt() ?: throw RuntimeException("numPartitions not set"),
                config["tell.partitionShift"]?.toInt() ?: throw RuntimeException("partitionShift not set"))
    }

    override fun getRecordSetProvider(): ConnectorRecordSetProvider? {
        return TellRecordSetProvider(TellConnection.scanMemoryManager(config))
    }
}

class TellConnectionFactory : ConnectorFactory {
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
            return ImmutableList.of(type.cast(TellConnectionFactory()));
        }
        return ImmutableList.of();
    }
}