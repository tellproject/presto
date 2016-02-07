package ch.ethz.tell.presto.java;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;


public class TellPlugin implements Plugin {
    private Map<String, String> optionalConfig = ImmutableMap.of();

    @Override
    public synchronized void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public <T> List<T> getServices(Class<T> type) {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(new TellConnection(optionalConfig)));
        }
        return ImmutableList.of();
    }
}
