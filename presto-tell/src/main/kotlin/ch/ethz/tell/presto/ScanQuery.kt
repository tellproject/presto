package ch.ethz.tell.presto

import ch.ethz.tell.*
import ch.ethz.tell.ScanQuery.CmpType.*
import com.facebook.presto.spi.ColumnHandle
import com.facebook.presto.spi.predicate.*
import com.facebook.presto.spi.predicate.Marker.Bound.EXACTLY
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.collect.ImmutableList
import java.util.*

class TellPredicate {
    @get:JsonGetter
    val cmpType: ScanQuery.CmpType
    @get:JsonGetter
    val field: Short
    @get:JsonGetter
    val value: Any?

    @JsonCreator
    constructor(@JsonProperty("cmpType") cmpType: ScanQuery.CmpType,
                @JsonProperty("field") field: Short,
                @JsonProperty("value") value: Any?) {
        this.cmpType = cmpType
        this.field = field
        this.value = value
    }
}

fun Schema.typeOf(id: Short): Field.FieldType {
    val i = id.toInt()
    val numFixedSize = this.fixedSizeFields().size
    if (id < numFixedSize) {
        return this.fixedSizeFields()[i]
    }
    return this.variableSizedFields()[i - numFixedSize]
}

class TellScanQuery {
    @get:JsonGetter
    val tableHandle: TellTableHandle
    @get:JsonGetter
    val queryType: ScanQuery.QueryType
    @get:JsonGetter
    val projections: MutableSet<ColumnHandle>
    @get:JsonGetter
    val selections: MutableList<MutableList<TellPredicate>>

    val table: Table
        get() = tableHandle.table

    fun create(partitionShift: Int, partitionKey: Int, partitionValue: Int): ScanQuery {
        val schema = table.schema
        val query = ScanQuery(table.tableName, partitionShift, partitionKey, partitionValue)
        selections.forEach {
            val clause = CNFClause()
            it.forEach {
                clause.addPredicate(it.cmpType, it.field, toPredicateType(schema.typeOf(it.field), it.value))
            }
            query.addSelection(clause)
        }
        assert(projections.isEmpty() && queryType == ScanQuery.QueryType.FULL
                || !projections.isEmpty() && queryType == ScanQuery.QueryType.PROJECTION)
        projections.forEach {
            if (it !is TellColumnHandle) throw RuntimeException("Unknown column type")
            val field = it.field
            query.addProjection(Projection(field.index, field.fieldName, field.fieldType, field.notNull))
        }
        return query
    }

    @JsonCreator
    constructor(@JsonProperty("tableHandle") tableHandle: TellTableHandle,
                @JsonProperty("queryType") queryType: ScanQuery.QueryType,
                @JsonProperty("projections") projections: MutableSet<ColumnHandle>,
                @JsonProperty("selections") selections: MutableList<MutableList<TellPredicate>>) {
        this.tableHandle = tableHandle
        this.queryType = queryType
        this.projections = projections
        this.selections = selections
    }

    constructor(tableHandle: TellTableHandle,
                domain: TupleDomain<ColumnHandle>,
                desiredColumns: Optional<MutableSet<ColumnHandle>>) {
        this.projections = desiredColumns.orElse(HashSet<ColumnHandle>())
        this.queryType =
                if (projections.isEmpty())
                    ScanQuery.QueryType.FULL
                else
                    ScanQuery.QueryType.PROJECTION
        this.tableHandle = tableHandle
        this.selections = ArrayList()
        domain.domains.ifPresent {
            it.forEach {
                val clause = ArrayList<TellPredicate>()
                var didAdd = false
                val field = (it.key as? TellColumnHandle)?.field ?: throw RuntimeException("Unknown field type")
                if (it.value.isOnlyNull) {
                    clause.add(TellPredicate(IS_NULL, field.index, null))
                } else if (it.value == Domain.notNull(it.value.type)) {
                    clause.add(TellPredicate(IS_NOT_NULL, field.index, null))
                } else if (it.value.isSingleValue) {
                    clause.add(TellPredicate(EQUAL, field.index, it.value.values.singleValue))
                } else if (it.value.values is SortedRangeSet) {
                    didAdd = true
                    val ranges = it.value.values.ranges
                    if (ranges.rangeCount > 4) throw RuntimeException("Unsupported number of ranges")
                    val upperLowerBuilder = ImmutableList.builder<Range>()
                    ranges.orderedRanges.forEach {
                        if (it.low.valueBlock.isPresent && it.high.valueBlock.isPresent) {
                            upperLowerBuilder.add(it)
                        } else if (it.low.valueBlock.isPresent) {
                            val cmpType = if (it.low.bound == EXACTLY) GREATER_EQUAL else GREATER
                            var innerClause = ArrayList<TellPredicate>()
                            innerClause.add(TellPredicate(cmpType, field.index, it.low.value))
                            selections.add(innerClause)
                        } else if (it.high.valueBlock.isPresent) {
                            val cmpType = if (it.high.bound == EXACTLY) LESS_EQUAL else LESS
                            var innerClause = ArrayList<TellPredicate>()
                            innerClause.add(TellPredicate(cmpType, field.index, it.high.value))
                            selections.add(innerClause)
                        } else {
                            throw RuntimeException("Invalid range")
                        }
                    }
                    val upperLower = upperLowerBuilder.build()
                    for (i in 0..upperLower.size - 1) {
                        val low = upperLower[i].low
                        val lowCmp = if (low.bound == EXACTLY) GREATER_EQUAL else GREATER
                        for (j in i..upperLower.size - 1) {
                            val jlow = upperLower[j].low
                            val jlowCmp = if (jlow.bound == EXACTLY) GREATER_EQUAL else GREATER
                            val jhigh = upperLower[j].high
                            val jhighCmp = if (jhigh.bound == EXACTLY) LESS_EQUAL else LESS

                            var innerClause = ArrayList<TellPredicate>()
                            innerClause.add(TellPredicate(lowCmp, field.index, low.value))
                            innerClause.add(TellPredicate(jlowCmp, field.index, jlow.value))
                            selections.add(innerClause)

                            innerClause = ArrayList<TellPredicate>()
                            innerClause.add(TellPredicate(lowCmp, field.index, low.value))
                            innerClause.add(TellPredicate(jhighCmp, field.index, jhigh.value))
                            selections.add(innerClause)
                        }

                        val high = upperLower[i].high
                        val highCmp = if (high.bound == EXACTLY) LESS_EQUAL else LESS
                        for (j in i..upperLower.size - 1) {
                            val jlow = upperLower[j].low
                            val jlowCmp = if (jlow.bound == EXACTLY) GREATER_EQUAL else GREATER
                            val jhigh = upperLower[j].high
                            val jhighCmp = if (jhigh.bound == EXACTLY) LESS_EQUAL else LESS

                            var innerClause = ArrayList<TellPredicate>()
                            innerClause.add(TellPredicate(highCmp, field.index, high.value))
                            innerClause.add(TellPredicate(jlowCmp, field.index, jlow.value))
                            selections.add(innerClause)

                            innerClause = ArrayList<TellPredicate>()
                            innerClause.add(TellPredicate(highCmp, field.index, high.value))
                            innerClause.add(TellPredicate(jhighCmp, field.index, jhigh.value))
                            selections.add(innerClause)
                        }
                    }
                } else if (it.value.values is EquatableValueSet) {
                    val values = it.value.values.discreteValues
                    if (values.values.size > 20) throw RuntimeException("Can not support so many distinct values")
                    if (values.isWhiteList) {
                        values.values.forEach {
                            clause.add(TellPredicate(EQUAL, field.index, it))
                        }
                    } else {
                        didAdd = true
                        values.values.forEach {
                            val innerClause = ArrayList<TellPredicate>()
                            innerClause.add(TellPredicate(NOT_EQUAL, field.index, it))
                            selections.add(innerClause)
                        }
                    }
                } else {
                    throw RuntimeException("Unsupported domain")
                }
                if (!didAdd) selections.add(clause)
            }
        }
    }
}
