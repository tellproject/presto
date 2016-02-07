package ch.ethz.tell.presto.java;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.jetbrains.annotations.Contract;

public class TellHandleResolver implements ConnectorHandleResolver {
    @Contract(pure = true)
    @Override
    public final Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return TellTableHandle.class;
    }

    @Contract(pure = true)
    @Override
    public final Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return TellTableLayoutHandle.class;
    }

    @Contract(pure = true)
    @Override
    public final Class<? extends ColumnHandle> getColumnHandleClass() {
        return TellColumnHandle.class;
    }

    @Contract(pure = true)
    @Override
    public final Class<? extends ConnectorSplit> getSplitClass() {
        return TellSplit.class;
    }

    @Contract(pure = true)
    @Override
    public final Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
        return TellTransaction.class;
    }
}
