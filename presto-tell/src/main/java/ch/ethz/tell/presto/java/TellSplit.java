package ch.ethz.tell.presto.java;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.Contract;

import java.util.List;

public class TellSplit implements ConnectorSplit {
    @Contract(value = " -> false", pure = true)
    @Override
    public final boolean isRemotelyAccessible() {
        return true;
    }

    @Contract(value = " -> null", pure = true)
    @Override
    public final List<HostAddress> getAddresses() {
        return ImmutableList.of();
    }

    @Contract(value = " -> null", pure = true)
    @Override
    public final Object getInfo() {
        return this;
    }
}
