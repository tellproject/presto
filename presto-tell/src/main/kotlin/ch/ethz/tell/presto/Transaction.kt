package ch.ethz.tell.presto

import ch.ethz.tell.Transaction
import com.facebook.presto.spi.connector.ConnectorTransactionHandle

class TellTransactionHandle(val transaction: Transaction) : ConnectorTransactionHandle
