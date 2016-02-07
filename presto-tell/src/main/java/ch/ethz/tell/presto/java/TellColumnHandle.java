package ch.ethz.tell.presto.java;

import ch.ethz.tell.Field;
import com.facebook.presto.spi.ColumnHandle;
import org.jetbrains.annotations.Contract;


public class TellColumnHandle implements ColumnHandle {
    private final Field field;

    TellColumnHandle(Field field) {
        this.field = field;
    }

    @Contract(pure = true)
    final Field field() {
        return field;
    }
}
