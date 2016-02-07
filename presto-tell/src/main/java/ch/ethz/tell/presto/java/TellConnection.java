package ch.ethz.tell.presto.java;

import ch.ethz.tell.ClientManager;
import ch.ethz.tell.Transaction;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.transaction.IsolationLevel;

import java.util.Map;

public class TellConnection implements ConnectorFactory, Connector {
    private Map<String, String> optionalConfig;
    private ClientManager clientManager = null;

    public TellConnection(Map<String, String> config) {
        optionalConfig = config;
    }

    @Override
    public final String getName() {
        return "tell";
    }

    @Override
    public final ConnectorHandleResolver getHandleResolver() {
        return new TellHandleResolver();
    }

    @Override
    public final Connector create(String connectorId, Map<String, String> config) {
        if (clientManager == null) {
            synchronized (this) {
                if (clientManager == null) {
                    clientManager = new ClientManager(config.get("tell.commitManager"), config.get("tell.storages"));
                }
            }
        }
        return this;
    }

    @Override
    public final ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        if (!readOnly) {
            throw new UnsupportedOperationException();
        }
        return new TellTransaction(Transaction.startTransaction(clientManager));
    }

    @Override
    public final ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return new TellMetaData((TellTransaction) transactionHandle);
    }

    @Override
    public final ConnectorSplitManager getSplitManager() {
        return null;
    }

    @Override
    public final void commit(ConnectorTransactionHandle transactionHandle) {
        TellTransaction tx = (TellTransaction) transactionHandle;
        tx.commit();
    }

    @Override
    public void shutdown() {
        clientManager.close();
        clientManager = null;
    }
}
