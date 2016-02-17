package ch.ethz.kudu

import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.ConnectorMetadata
import com.facebook.presto.spi.type.*
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.stumbleupon.async.Deferred
import org.kududb.ColumnSchema
import org.kududb.Type
import org.kududb.Type.*
import org.kududb.client.KuduTable
import java.util.*

class KuduColumnHandle : ColumnHandle {
    val table: KuduTable;
    val column: ColumnSchema

    @get:JsonGetter
    val tableName: String
        get() = table.name

    @get:JsonGetter
    val name: String
        get() = column.name

    constructor(table: KuduTable, column: ColumnSchema) {
        this.table = table
        this.column = column
    }

    @JsonCreator
    constructor(@JsonProperty("tableName") tableName: String,
                @JsonProperty("name") name: String) {
        val table = ClientSingleton.client!!.openTable(tableName).join()
        this.table = table
        this.column = table.schema.getColumn(name)
    }
}

class KuduTableLayoutHandle : ConnectorTableLayoutHandle {
    @get:JsonGetter
    val query: KuduScanQuery

    @JsonCreator
    constructor(@JsonProperty("query") query: KuduScanQuery) {
        this.query = query
    }
}

class KuduTableHandle(private val tableFuture: Deferred<KuduTable>) : ConnectorTableHandle {
    @get:JsonGetter
    val tableName: String
        get() = table.name

    val table: KuduTable
        get() = tableFuture.join()

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
        return ImmutableList.of(DefaultSchema.name)
    }

    override fun getTableHandle(session: ConnectorSession?, tableName: SchemaTableName): ConnectorTableHandle? {
        if (tableName.schemaName != DefaultSchema.name) throw RuntimeException("Schemas not supported")
        return KuduTableHandle(ClientSingleton.client!!.openTable(tableName.tableName))
    }

    override fun getTableLayouts(session: ConnectorSession?,
                                 table: ConnectorTableHandle?,
                                 constraint: Constraint<ColumnHandle>?,
                                 desiredColumns: Optional<MutableSet<ColumnHandle>>): MutableList<ConnectorTableLayoutResult>? {
        if (table !is KuduTableHandle) throw RuntimeException("Unknown table handle")
        if (constraint == null) throw RuntimeException("No constraints")
        val query = KuduScanQuery(table, constraint.summary, desiredColumns)
        return ImmutableList.of(
                ConnectorTableLayoutResult(ConnectorTableLayout(KuduTableLayoutHandle(query)),
                        query.unenforcedConstraints())
        )
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
        return ImmutableList.copyOf(ClientSingleton.client!!.tablesList.join().tablesList.map {
            SchemaTableName(DefaultSchema.name, it)
        })
    }

    override fun getColumnHandles(session: ConnectorSession?,
                                  tableHandle: ConnectorTableHandle?): MutableMap<String, ColumnHandle>? {
        if (tableHandle !is KuduTableHandle) throw RuntimeException("Unknown table handle")
        val builder = ImmutableMap.builder<String, ColumnHandle>()
        tableHandle.table.schema.columns.forEach {
            builder.put(it.name, KuduColumnHandle(tableHandle.table, it))
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
        ClientSingleton.client!!.tablesList.join().tablesList.forEach {
            if (it.startsWith(prefix.tableName)) {
                val tableF = ClientSingleton.client!!.openTable(it)
                val cBuilder = ImmutableList.builder<ColumnMetadata>()
                val table = tableF.join()
                table.schema.columns.forEach {
                    cBuilder.add(ColumnMetadata(it.name, it.type.prestoType(), it.isKey))
                }
                builder.put(SchemaTableName(DefaultSchema.name, table.name), cBuilder.build())
            }
        }
        return builder.build()
    }

}
