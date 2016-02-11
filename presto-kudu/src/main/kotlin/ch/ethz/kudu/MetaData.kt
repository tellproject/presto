package ch.ethz.kudu

import ch.ethz.kudu.ClientSingleton
import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.ConnectorMetadata
import com.facebook.presto.spi.type.*
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import org.kududb.ColumnSchema
import org.kududb.Type
import org.kududb.Type.*
import org.kududb.client.KuduTable
import java.util.*

class KuduColumnHandle(val column: ColumnSchema) : ColumnHandle

class KuduTableLayoutHandle : ConnectorTableLayoutHandle

class KuduTableHandle(val table: KuduTable) : ConnectorTableHandle {
    @get:JsonGetter
    val tableName: String
        get() = table.name

    @JsonCreator
    constructor(@JsonProperty("tableName") tableName: String)
        : this(ClientSingleton.client?.openTable(tableName) ?: throw RuntimeException("Client is not initialized"))
}

fun Type.prestoType(): com.facebook.presto.spi.type.Type {
    return when (this) {
        BOOL -> BooleanType.BOOLEAN
        INT8, INT16, INT32, INT64 -> BigintType.BIGINT
        FLOAT, DOUBLE -> DoubleType.DOUBLE
        TIMESTAMP -> TimestampType.TIMESTAMP
        BINARY -> VarbinaryType.VARBINARY
        STRING -> VarcharType.VARCHAR
    }
}

object DefaultSchema {
    val name = "default"
}

class KuduMetadata(val transactionHandle: KuduTransactionHandle) : ConnectorMetadata {
    override fun listSchemaNames(session: ConnectorSession?): MutableList<String> {
        val tables = transactionHandle.client.tablesList
        return tables.tablesList
    }

    override fun getTableHandle(session: ConnectorSession?, tableName: SchemaTableName): ConnectorTableHandle? {
        if (tableName.schemaName != DefaultSchema.name) throw RuntimeException("Schemas not supported")
        return KuduTableHandle(transactionHandle.client.openTable(tableName.tableName))
    }

    override fun getTableLayouts(session: ConnectorSession?,
                                 table: ConnectorTableHandle?,
                                 constraint: Constraint<ColumnHandle>?,
                                 desiredColumns: Optional<MutableSet<ColumnHandle>>?): MutableList<ConnectorTableLayoutResult>? {
        throw UnsupportedOperationException()
    }

    override fun getTableLayout(session: ConnectorSession?,
                                handle: ConnectorTableLayoutHandle?): ConnectorTableLayout? {
        return ConnectorTableLayout(handle)
    }

    override fun getTableMetadata(session: ConnectorSession?, table: ConnectorTableHandle?): ConnectorTableMetadata? {
        if (table !is KuduTableHandle) throw RuntimeException("Unknown table handle")
        val columns = getColumnHandles(session, table)
        val builder = ImmutableList.builder<ColumnMetadata>()
        columns?.forEach {
            builder.add(getColumnMetadata(session, table, it.value))
        }
        return ConnectorTableMetadata(SchemaTableName(DefaultSchema.name, table.tableName), builder.build())
    }

    override fun listTables(session: ConnectorSession?, schemaNameOrNull: String?): MutableList<SchemaTableName>? {
        if (schemaNameOrNull != null && schemaNameOrNull != DefaultSchema.name) {
            throw RuntimeException("Schemas not supported by kudu but $schemaNameOrNull was given")
        }
        return ImmutableList.copyOf(listSchemaNames(session).map { SchemaTableName(DefaultSchema.name, it) })
    }

    override fun getColumnHandles(session: ConnectorSession?,
                                  tableHandle: ConnectorTableHandle?): MutableMap<String, ColumnHandle>? {
        if (tableHandle !is KuduTableHandle) throw RuntimeException("Unknown table handle")
        val builder = ImmutableMap.builder<String, ColumnHandle>()
        tableHandle.table.schema.columns.forEach {
            builder.put(it.name, KuduColumnHandle(it))
        }
        return builder.build()
    }

    override fun getColumnMetadata(session: ConnectorSession?,
                                   tableHandle: ConnectorTableHandle?,
                                   columnHandle: ColumnHandle?): ColumnMetadata? {
        if (columnHandle !is KuduColumnHandle) throw RuntimeException("Unknown column handle")
        val column = columnHandle.column
        return ColumnMetadata(column.name, column.type.prestoType(), column.isKey)
    }

    override fun listTableColumns(session: ConnectorSession?,
                                  prefix: SchemaTablePrefix): MutableMap<SchemaTableName, MutableList<ColumnMetadata>> {
        if (prefix.schemaName != DefaultSchema.name) {
            throw RuntimeException("Schemas not supported by kudu but ${prefix.schemaName} was given")
        }
        val builder = ImmutableMap.builder<SchemaTableName, MutableList<ColumnMetadata>>()
        transactionHandle.client.tablesList.tablesList.forEach {
            if (it.startsWith(prefix.tableName)) {
                val table = transactionHandle.client.openTable(it)
                val cBuilder = ImmutableList.builder<ColumnMetadata>()
                table.schema.columns.forEach {
                    cBuilder.add(ColumnMetadata(it.name, it.type.prestoType(), it.isKey))
                }
                builder.put(SchemaTableName(DefaultSchema.name, table.name), cBuilder.build())
            }
        }
        return builder.build()
    }

}
