package ch.ethz.kudu

import com.facebook.presto.spi.ColumnHandle
import com.facebook.presto.spi.predicate.Domain
import com.facebook.presto.spi.predicate.TupleDomain
import com.google.common.collect.ImmutableList
import org.kududb.client.AsyncKuduScanner
import java.util.*

class KuduScanQuery(val tableHandle: KuduTableHandle,
                    val domain: TupleDomain<ColumnHandle>,
                    val desiredColumns: Optional<MutableSet<ColumnHandle>>) {

    fun create(): AsyncKuduScanner {
        tableHandle.table.partitionSchema
        val scanner = ClientSingleton.client!!.newScannerBuilder(tableHandle.table)
        // projection
        desiredColumns.ifPresent {
            scanner.setProjectedColumnNames(ImmutableList.copyOf(
                    it.map {
                        if (it !is KuduColumnHandle) throw RuntimeException("Unknown column handle")
                        it.column.name
                    }
            ))
        }
        val nonenforced = TupleDomain.none<ColumnHandle>()
        domain.domains.ifPresent {
            val domains = it
            domains.forEach {
                val column = (it.key as? KuduColumnHandle)?.column ?: throw RuntimeException("Unknown column handle")
                if (it.value.isOnlyNull) {
                    // TODO: Can not enforce
                } else if (it.value == Domain.notNull(it.value.type)) {
                    // TODO: Can not enforce
                } else if (it.value.isSingleValue) {
                    
                }
            }
        }
        return scanner.build()
    }
}
