package ch.ethz.kudu

import com.facebook.presto.spi.ColumnHandle
import com.facebook.presto.spi.predicate.*
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import org.kududb.ColumnSchema
import org.kududb.Type
import org.kududb.client.AsyncKuduScanner
import org.kududb.client.ColumnRangePredicate
import org.kududb.client.shaded.com.google.protobuf.ZeroCopyLiteralByteString
import org.kududb.tserver.Tserver.ColumnRangePredicatePB
import java.util.*

fun predicate(column: ColumnSchema, lower: Any?, upper: Any?): ColumnRangePredicate {
    val res = ColumnRangePredicate(column)
    when (column.type) {
        null -> {
            throw RuntimeException("type must not be null")
        }
        Type.BOOL -> {
            if (lower != null) res.setLowerBound(lower as Boolean)
            if (upper != null) res.setUpperBound(upper as Boolean)
        }
        Type.INT8 -> {
            if (upper != null) res.setUpperBound((upper as Long).toByte())
            if (lower != null) res.setLowerBound((lower as Long).toByte())
        }
        Type.INT16 -> {
            if (lower != null) res.setLowerBound((lower as Long).toShort())
            if (upper != null) res.setUpperBound((upper as Long).toShort())
        }
        Type.INT32 -> {
            if (lower != null) res.setLowerBound((lower as Long).toInt())
            if (upper != null) res.setUpperBound((upper as Long).toInt())
        }
        Type.INT64 -> {
            if (lower != null) res.setLowerBound(lower as Long)
            if (upper != null) res.setUpperBound(upper as Long)
        }
        Type.FLOAT -> {
            if (lower != null) res.setLowerBound((lower as Double).toFloat())
            if (upper != null) res.setUpperBound((upper as Double).toFloat())
        }
        Type.DOUBLE -> {
            if (lower != null) res.setLowerBound(lower as Double)
            if (upper != null) res.setUpperBound(upper as Double)
        }
        Type.BINARY -> {
            if (lower != null) res.lowerBound = lower as ByteArray
            if (upper != null) res.upperBound = upper as ByteArray
        }
        Type.STRING -> {
            if (upper != null) res.setUpperBound(upper as String)
            if (lower != null) res.setLowerBound(lower as String)
        }
        Type.TIMESTAMP -> {
            if (lower != null) res.setLowerBound(lower as Long)
            if (upper != null) res.setUpperBound(upper as Long)
        }
    }
    return res
}

class KuduScanQuery {

    @get:JsonGetter
    val tableHandle: KuduTableHandle
    @get:JsonGetter
    val domain: TupleDomain<ColumnHandle>
    @get:JsonGetter
    val desiredColumns: Optional<MutableSet<ColumnHandle>>

    @JsonCreator
    constructor(@JsonProperty("tableHandle") tableHandle: KuduTableHandle,
                @JsonProperty("domain") domain: TupleDomain<ColumnHandle>,
                @JsonProperty("desiredColumns") desiredColumns: Optional<MutableSet<ColumnHandle>>)
    {
        this.tableHandle = tableHandle
        this.domain = domain
        this.desiredColumns = desiredColumns
    }

    fun unenforcedConstraints(): TupleDomain<ColumnHandle>? {
        val builder = ImmutableMap.builder<ColumnHandle, Domain>()
        domain.domains.ifPresent {
            it.forEach {
                if (it.value.isOnlyNull) {
                    builder.put(it)
                } else if (it.value == Domain.notNull(it.value.type)) {
                    builder.put(it)
                } else if (it.value.values is SortedRangeSet) {
                    val ranges = it.value.values.ranges
                    val rangeBbuilder = ImmutableList.builder<Range>()
                    ranges.orderedRanges.forEach {
                        val inclusiveLower = !it.low.valueBlock.isPresent || it.low.bound == Marker.Bound.EXACTLY
                        val inclusiveUpper = !it.high.valueBlock.isPresent || it.high.bound == Marker.Bound.EXACTLY
                        if (!inclusiveLower || !inclusiveUpper) {
                            rangeBbuilder.add(it)
                        }
                    }
                    val nonEnforcedRanges = rangeBbuilder.build()
                    if (!nonEnforcedRanges.isEmpty()) {
                        builder.put(it.key, Domain.create(ValueSet.copyOfRanges(
                                it.value.type,
                                nonEnforcedRanges
                        ), it.value.isNullAllowed))
                    }
                } else if (it.value.values is EquatableValueSet) {
                    val values = it.value.values.discreteValues
                    if (!values.isWhiteList) {
                        builder.put(it)
                    }
                }
            }
        }
        return TupleDomain.withColumnDomains(builder.build())
    }

    fun create(lowerBound: ByteArray, upperBound: ByteArray): AsyncKuduScanner {
        tableHandle.table.partitionSchema
        val scanner = ClientSingleton.client!!.newScannerBuilder(tableHandle.table)
        // range partition
        if (!lowerBound.isEmpty() || !upperBound.isEmpty()) {
            val pbBuilder = ColumnRangePredicatePB.newBuilder()
            pbBuilder.lowerBound = ZeroCopyLiteralByteString.wrap(lowerBound)
            pbBuilder.upperBound = ZeroCopyLiteralByteString.wrap(upperBound)
            scanner.addColumnRangePredicatesRaw(pbBuilder.build().toByteArray())
        }
        // projection
        desiredColumns.ifPresent {
            val builder = ImmutableList.builder<String>()
            it.forEach {
                if (it !is KuduColumnHandle) throw RuntimeException("Unknown column handle")
                if (it.column.isKey) builder.add(it.column.name)
            }
            it.forEach {
                if (it !is KuduColumnHandle) throw RuntimeException("Unknown column handle")
                if (!it.column.isKey) builder.add(it.column.name)
            }
            scanner.setProjectedColumnNames(builder.build())
        }
        domain.domains.ifPresent {
            val domains = it
            domains.forEach {
                val column = (it.key as? KuduColumnHandle)?.column ?: throw RuntimeException("Unknown column handle")
                if (it.value.isOnlyNull) {
                    // Can not enforce
                } else if (it.value == Domain.notNull(it.value.type)) {
                    // Can not enforce
                } else if (it.value.isSingleValue) {
                    scanner.addColumnRangePredicate(predicate(column, it.value.singleValue, it.value.singleValue))
                } else if (it.value.values is SortedRangeSet) {
                    val ranges = it.value.values.ranges
                    ranges.orderedRanges.forEach {
                        val inclusiveLower = !it.low.valueBlock.isPresent || it.low.bound == Marker.Bound.EXACTLY
                        val inclusiveUpper = !it.high.valueBlock.isPresent || it.high.bound == Marker.Bound.EXACTLY
                        if (inclusiveLower && inclusiveUpper) {
                            scanner.addColumnRangePredicate(predicate(
                                    column,
                                    if (it.low.valueBlock.isPresent) it.low.value else null,
                                    if (it.high.valueBlock.isPresent) it.high.value else null
                            ))
                        } else {
                            // Can not enforce
                        }
                    }
                } else if (it.value.values is EquatableValueSet) {
                    val values = it.value.values.discreteValues
                    if (values.values.size > 20) throw RuntimeException("Can not support so many distinct values")
                    if (values.isWhiteList) {
                        values.values.forEach {
                            scanner.addColumnRangePredicate(predicate(column, it, it))
                        }
                    } else {
                        // Can not enforce
                        // can not support, as Kudu only supports including ranges
                    }
                } else {
                    throw RuntimeException("Unknown domain")
                }
            }
        }
        return scanner.build()
    }
}
