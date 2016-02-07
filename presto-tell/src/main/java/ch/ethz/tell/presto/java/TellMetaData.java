package ch.ethz.tell.presto.java;

import ch.ethz.tell.Field;
import ch.ethz.tell.Schema;
import ch.ethz.tell.Transaction;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TellMetaData implements ConnectorMetadata {
    private final TellTransaction transaction;

    public TellMetaData(TellTransaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        // TODO: Find a hack!
        return ImmutableList.of();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        String tname = tableName.getTableName();
        Transaction tx = transaction.transaction();
        Schema schema = tx.schemaForTable(tname);
        return new TellTableHandle(schema, tname);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
                                                            ConnectorTableHandle table,
                                                            Constraint<ColumnHandle> constraint,
                                                            Optional<Set<ColumnHandle>> desiredColumns)
    {
        return null;
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        Map<String, ColumnHandle> handles = getColumnHandles(session, table);
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        handles.forEach((name, field) -> columns.add(getColumnMetadata(session, table, field)));
        return new ConnectorTableMetadata(new SchemaTableName("", ((TellTableHandle)table).name()), columns.build());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        return null;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        TellTableHandle handle = (TellTableHandle) tableHandle;
        Schema schema = handle.schema();
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (String name : schema.getFieldNames()) {
            Field field = schema.getFieldByName(name);
            builder.put(name, new TellColumnHandle(field));
        }
        builder.put("__key", new PrimaryKeyColumnHandle());
        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        if (columnHandle instanceof PrimaryKeyColumnHandle) {
            return new ColumnMetadata("__key", BigintType.BIGINT, true);
        }
        Field field = ((TellColumnHandle) columnHandle).field();
        Type t = null;
        switch (field.fieldType) {
            case NOTYPE:
                throw new RuntimeException("Unsupported type");
            case NULLTYPE:
                throw new RuntimeException("Field cannot have null type");
            case SMALLINT:
                t = BigintType.BIGINT;
                break;
            case INT:
                t = BigintType.BIGINT;
                break;
            case BIGINT:
                t = BigintType.BIGINT;
                break;
            case FLOAT:
                t = DoubleType.DOUBLE;
                break;
            case DOUBLE:
                t = DoubleType.DOUBLE;
                break;
            case TEXT:
                t = VarcharType.VARCHAR;
                break;
            case BLOB:
                t = VarbinaryType.VARBINARY;
                break;
        }
        return new ColumnMetadata(field.fieldName, t, false);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        return null;
    }
}
