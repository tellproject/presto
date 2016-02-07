package ch.ethz.tell.presto.java;

import ch.ethz.tell.Transaction;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class TellTransaction implements ConnectorTransactionHandle {
    private Transaction tx;

    TellTransaction(Transaction tx) {
        this.tx = tx;
    }

    final Transaction TellTransaction() {
        return tx;
    }

    final void commit() {
        tx.commit();
    }

    final Transaction transaction() {
        return tx;
    }
}
