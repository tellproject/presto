package ch.ethz.tell.presto

import ch.ethz.tell.Field
import ch.ethz.tell.Field.FieldType.*
import ch.ethz.tell.Table
import ch.ethz.tell.Transaction
import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.ConnectorMetadata
import com.facebook.presto.spi.predicate.TupleDomain
import com.facebook.presto.spi.type.*
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import java.util.*

object TableCache {
    var tables: ImmutableMap<String, Table>? = null
    var tableIds: ImmutableMap<Long, Table>? = null

    private fun init(transaction: Transaction) {
        if (tables == null) {
            synchronized(this) {
                if (tables == null) {
                    val builder = ImmutableMap.builder<String, Table>()
                    val idBuilder = ImmutableMap.builder<Long, Table>()
                    transaction.tables.forEach {
                        builder.put(it.tableName, it)
                        idBuilder.put(it.tableId, it)
                    }
                    tables = builder.build()
                    tableIds = idBuilder.build()
                }
            }
        }
    }

    fun openTable(transaction: Transaction, name: String): Table {
        init(transaction)
        val res = tables!![name] ?: throw RuntimeException("table $name does not exist")
        return res
    }

    fun openTable(transaction: Transaction, id: Long): Table {
        init(transaction)
        val res = tableIds!![id] ?: throw RuntimeException("table $id does not exist")
        return res
    }

    fun getTableNames(transaction: Transaction): ImmutableList<String> {
        init(transaction)
        val builder = ImmutableList.builder<String>()
        tables!!.forEach { builder.add(it.key) }
        return builder.build()
    }

    fun getTables(transaction: Transaction): ImmutableMap<String, Table> {
        init(transaction)
        return tables!!
    }
}

object DefaultSchema {
    val name = "default"
}

class TellTableHandle(val table: Table,
                      @get:com.fasterxml.jackson.annotation.JsonGetter val transactionId: Long) : ConnectorTableHandle {
    @JsonCreator
    constructor(@JsonProperty("tableId") tableId: Long, @JsonProperty("transactionId") transactionId: Long)
    : this(TableCache.openTable(Transaction.startTransaction(transactionId, TellConnection.clientManager),
            tableId), transactionId)

    @JsonGetter
    fun getTableId(): Long {
        return table.tableId
    }
}

class TellTableLayoutHandle : ConnectorTableLayoutHandle {
    @get:JsonGetter
    val scanQuery: TellScanQuery

    @JsonCreator
    constructor(@JsonProperty("scanQuery") scanQuery: TellScanQuery) {
        this.scanQuery = scanQuery
    }
}

val KeyName = "__key"

class TellColumnHandle : ColumnHandle {
    val field: Field?

    @JsonGetter("fieldName")
    fun getFieldName(): String {
        if (field == null) return KeyName
        return field.fieldName
    }

    @get:JsonGetter
    val tableId: Long

    constructor(field: Field?, tableId: Long) {
        this.field = field
        this.tableId = tableId
    }

    @JsonCreator
    constructor(@JsonProperty("fieldName") fieldName: String,
                @JsonProperty("tableId") tableId: Long) {
        this.tableId = tableId
        if (fieldName == KeyName)
            this.field = null
        else
            this.field = TableCache.tableIds!![tableId]?.schema?.getFieldByName(fieldName) ?: throw RuntimeException("Could not find table")
    }
}

fun Field.prestoType(): Type {
    return when (fieldType) {
        null -> throw RuntimeException("NULL")
        NOTYPE -> throw RuntimeException("NOTYPE")
        NULLTYPE -> throw RuntimeException("NULL")
        SMALLINT, INT, BIGINT -> BigintType.BIGINT
        FLOAT, DOUBLE -> DoubleType.DOUBLE
        TEXT -> VarcharType.VARCHAR
        BLOB -> VarbinaryType.VARBINARY
    }
}

class TellMetadata(val transaction: Transaction) : ConnectorMetadata {
    override fun listSchemaNames(session: ConnectorSession?): MutableList<String>? {
        return ImmutableList.of(DefaultSchema.name)
    }

    override fun getTableHandle(session: ConnectorSession?, tableName: SchemaTableName?): ConnectorTableHandle? {
        if (tableName != null && tableName.schemaName == DefaultSchema.name) {
            return TellTableHandle(TableCache.openTable(transaction, tableName.tableName), transaction.transactionId)
        }
        throw RuntimeException("tableName is null")
    }

    override fun getTableLayouts(session: ConnectorSession?,
                                 table: ConnectorTableHandle?,
                                 constraint: Constraint<ColumnHandle>,
                                 desiredColumns: Optional<MutableSet<ColumnHandle>>): MutableList<ConnectorTableLayoutResult> {
        if (table !is TellTableHandle) {
            val typename = table?.javaClass?.name ?: "null"
            throw RuntimeException("table is not from tell (type is $typename)")
        }
        val layout = ConnectorTableLayout(TellTableLayoutHandle(TellScanQuery(table, constraint.summary, desiredColumns)))
        return ImmutableList.of(ConnectorTableLayoutResult(layout, TupleDomain.all()))
    }

    override fun getTableLayout(session: ConnectorSession?,
                                handle: ConnectorTableLayoutHandle?): ConnectorTableLayout? {
        return ConnectorTableLayout(handle)
    }

    override fun getTableMetadata(session: ConnectorSession?, table: ConnectorTableHandle?): ConnectorTableMetadata? {
        if (table !is TellTableHandle) throw RuntimeException("table is not from tell")
        val columns = getColumnHandles(session, table)
        val builder = ImmutableList.builder<ColumnMetadata>()
        columns.forEach {
            builder.add(getColumnMetadata(session, table, it.value))
        }
        return ConnectorTableMetadata(SchemaTableName(DefaultSchema.name, table.table.tableName), builder.build())
    }

    override fun listTables(session: ConnectorSession?, schemaNameOrNull: String?): MutableList<SchemaTableName>? {
        if (schemaNameOrNull != null && schemaNameOrNull != DefaultSchema.name) {
            throw RuntimeException("Schemas not supported by tell but $schemaNameOrNull was given")
        }
        return ImmutableList.copyOf(
                TableCache.getTableNames(transaction).map { SchemaTableName(DefaultSchema.name, it) })
    }

    override fun getColumnHandles(session: ConnectorSession?,
                                  tableHandle: ConnectorTableHandle?): MutableMap<String, ColumnHandle> {
        if (tableHandle !is TellTableHandle) {
            throw RuntimeException("tableHandle is not from Tell")
        }
        val builder = ImmutableMap.builder<String, ColumnHandle>()
        builder.put(KeyName, TellColumnHandle(null, tableHandle.table.tableId))
        tableHandle.table.schema.fieldNames.forEach {
            builder.put(it, TellColumnHandle(tableHandle.table.schema.getFieldByName(it), tableHandle.table.tableId))
        }
        return builder.build()
    }

    private fun getMetadata(name: String, field: Field?): ColumnMetadata {
        if (field == null)
            return ColumnMetadata(name, BigintType.BIGINT, false)
        else
            return ColumnMetadata(name, field.prestoType(), false)
    }

    override fun getColumnMetadata(session: ConnectorSession?,
                                   tableHandle: ConnectorTableHandle?,
                                   columnHandle: ColumnHandle): ColumnMetadata {
        if (columnHandle !is TellColumnHandle) {
            throw RuntimeException("columnHandle is not from Tell")
        }
        if (columnHandle.field == null)
            return getMetadata(KeyName, null)
        else
            return getMetadata(columnHandle.field.fieldName, columnHandle.field)
    }

    override fun listTableColumns(session: ConnectorSession?,
                                  prefix: SchemaTablePrefix?): MutableMap<SchemaTableName, MutableList<ColumnMetadata>>? {
        if (prefix == null) {
            throw RuntimeException("prefix is null")
        }
        val builder = ImmutableMap.builder<SchemaTableName, MutableList<ColumnMetadata>>()
        val tables = TableCache.getTables(transaction)
        tables.forEach {
            if (it.key.startsWith(prefix.tableName)) {
                var b = ImmutableList.builder<ColumnMetadata>()
                val schema = it.value.schema
                b.add(getMetadata(KeyName, null))
                schema.fieldNames.forEach {
                    val field = schema.getFieldByName(it)
                    b.add(getMetadata(field.fieldName, field))
                }
                builder.put(SchemaTableName(DefaultSchema.name, it.key), b.build())
            }
        }
        return builder.build()
    }
}
