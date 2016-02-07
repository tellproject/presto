package ch.ethz.tell.presto.java;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mpilman on 05.02.16.
 */
public class TellSplitSource implements ConnectorSplitSource {
    @Override
    public String getDataSourceName() {
        return null;
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize) {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isFinished() {
        return false;
    }
}
