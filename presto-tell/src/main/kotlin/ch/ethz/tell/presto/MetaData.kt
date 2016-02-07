package ch.ethz.tell.presto

import ch.ethz.tell.Field
import ch.ethz.tell.Field.FieldType.*
import ch.ethz.tell.Table
import ch.ethz.tell.Transaction
import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.ConnectorMetadata
import com.facebook.presto.spi.type.BigintType
import com.facebook.presto.spi.type.DoubleType
import com.facebook.presto.spi.type.VarbinaryType
import com.facebook.presto.spi.type.VarcharType
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import java.util.*

object TableCache {
    private var tables: ImmutableMap<String, Table>? = null

    private fun init(transaction: Transaction) {
        if (tables == null) {
            synchronized(this) {
                if (tables == null) {
                    val builder = ImmutableMap.builder<String, Table>()
                    transaction.tables.forEach {
                        builder.put(it.tableName, it)
                    }
                    tables = builder.build()
                }
            }
        }
    }

    fun openTable(transaction: Transaction, name: String): Table {
        init(transaction)
        val res = tables!![name] ?: throw RuntimeException("table $name does not exist")
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

class TellTableHandle(val table: Table) : ConnectorTableHandle

class PrimaryKeyColumnHandle : ColumnHandle
class TellColumnHandle(val field: Field) : ColumnHandle

class TellMetadata(val transaction: Transaction) : ConnectorMetadata {
    override fun listSchemaNames(session: ConnectorSession?): MutableList<String>? {
        return ImmutableList.of("")
    }

    override fun getTableHandle(session: ConnectorSession?, tableName: SchemaTableName?): ConnectorTableHandle? {
        if (tableName != null) {
            return TellTableHandle(TableCache.openTable(transaction, tableName.tableName))
        }
        throw RuntimeException("tableName is null")
    }

    override fun getTableLayouts(session: ConnectorSession?, table: ConnectorTableHandle?, constraint: Constraint<ColumnHandle>?, desiredColumns: Optional<MutableSet<ColumnHandle>>?): MutableList<ConnectorTableLayoutResult>? {
        throw UnsupportedOperationException()
    }

    override fun getTableLayout(session: ConnectorSession?, handle: ConnectorTableLayoutHandle?): ConnectorTableLayout? {
        return ConnectorTableLayout(handle)
    }

    override fun getTableMetadata(session: ConnectorSession?, table: ConnectorTableHandle?): ConnectorTableMetadata? {
        if (table !is TellTableHandle) throw RuntimeException("table is not from tell")
        val columns = getColumnHandles(session, table)
        val builder = ImmutableList.builder<ColumnMetadata>()
        columns.forEach {
            builder.add(getColumnMetadata(session, table, it.value))
        }
        return ConnectorTableMetadata(SchemaTableName("", table.table.tableName), builder.build())
    }

    override fun listTables(session: ConnectorSession?, schemaNameOrNull: String?): MutableList<SchemaTableName>? {
        if (schemaNameOrNull != null || schemaNameOrNull != "") {
            throw RuntimeException("Schemas not supported by tell but $schemaNameOrNull was given")
        }
        return ImmutableList.copyOf(TableCache.getTableNames(transaction).map { SchemaTableName("", it) })
    }

    override fun getColumnHandles(session: ConnectorSession?, tableHandle: ConnectorTableHandle?): MutableMap<String, ColumnHandle> {
        if (tableHandle !is TellTableHandle) {
            throw RuntimeException("tableHandle is not from Tell")
        }
        val builder = ImmutableMap.builder<String, ColumnHandle>()
        builder.put("__key", PrimaryKeyColumnHandle())
        tableHandle.table.schema.fieldNames.forEach {
            builder.put(it, TellColumnHandle(tableHandle.table.schema.getFieldByName(it)))
        }
        return builder.build()
    }

    private fun getMetadata(name: String, fieldType: Field.FieldType): ColumnMetadata {
        val type = when (fieldType) {
            NOTYPE -> throw RuntimeException("NOTYPE")
            NULLTYPE -> throw RuntimeException("NULL")
            SMALLINT, INT, BIGINT -> BigintType.BIGINT
            FLOAT, DOUBLE -> DoubleType.DOUBLE
            TEXT -> VarcharType.VARCHAR
            BLOB -> VarbinaryType.VARBINARY
        }
        return ColumnMetadata(name, type, false)
    }

    override fun getColumnMetadata(session: ConnectorSession?, tableHandle: ConnectorTableHandle?, columnHandle: ColumnHandle): ColumnMetadata? {
        if (columnHandle is PrimaryKeyColumnHandle) {
            return ColumnMetadata("__key", BigintType.BIGINT, true);
        }
        if (columnHandle !is TellColumnHandle) {
            throw RuntimeException("columnHandle is not from Tell")
        }
        return getMetadata(columnHandle.field.fieldName, columnHandle.field.fieldType)
    }

    override fun listTableColumns(session: ConnectorSession?, prefix: SchemaTablePrefix?): MutableMap<SchemaTableName, MutableList<ColumnMetadata>>? {
        if (prefix == null) {
            throw RuntimeException("prefix is null")
        }
        val builder = ImmutableMap.builder<SchemaTableName, MutableList<ColumnMetadata>>()
        val tables = TableCache.getTables(transaction)
        tables.forEach {
            if (it.key.startsWith(prefix.tableName)) {
                var b = ImmutableList.builder<ColumnMetadata>()
                b.add(ColumnMetadata("__key", BigintType.BIGINT, true))
                val schema = it.value.schema
                schema.fieldNames.forEach {
                    val field = schema.getFieldByName(it)
                    b.add(getMetadata(field.fieldName, field.fieldType))
                }
                builder.put(SchemaTableName("", it.key), b.build())
            }
        }
        return builder.build()
    }
}
