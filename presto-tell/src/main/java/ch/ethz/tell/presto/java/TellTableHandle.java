package ch.ethz.tell.presto.java;

import ch.ethz.tell.Schema;
import com.facebook.presto.spi.ConnectorTableHandle;

public class TellTableHandle implements ConnectorTableHandle {

    private final Schema schema;
    private final String name;

    TellTableHandle(Schema schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    final Schema schema() {
        return schema;
    }

    final String name() {
        return name;
    }
}
