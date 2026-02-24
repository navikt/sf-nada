package no.nav.sf.nada.bulk

import com.google.cloud.bigquery.TableId
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import mu.KotlinLogging
import no.nav.sf.nada.HttpCalls.doSFBulkJobResultQuery
import no.nav.sf.nada.HttpCalls.doSFBulkJobStatusQuery
import no.nav.sf.nada.HttpCalls.doSFBulkStartQuery
import no.nav.sf.nada.TableDef
import no.nav.sf.nada.application
import no.nav.sf.nada.parseCSVToJsonArrays
import no.nav.sf.nada.remapAndSendRecords
import org.http4k.core.HttpHandler
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.File
import java.lang.Exception
import java.lang.RuntimeException

class OperationInfo {
    var preparingBulk: Boolean = false
    var transfering: Boolean = false
    var processedRecords: Long = 0L
    var transferDone: Boolean = false
    var jobId: String = ""
    var jobComplete: Boolean = false
    var transferReport: String = ""
    var expectedCount: Long = -1L
}

object BulkOperation {
    private val log = KotlinLogging.logger { }

    val operationInfo: MutableMap<String, MutableMap<String, OperationInfo>> = mutableMapOf()

    fun initOperationInfo(mapDef: Map<String, Map<String, TableDef>>) {
        operationInfo.clear()
        mapDef.keys.forEach { dataset ->
            operationInfo[dataset] = mutableMapOf()
            mapDef[dataset]!!.keys.forEach { table ->
                operationInfo[dataset]!![table] = OperationInfo()
            }
        }
    }

    val performBulkHandler: HttpHandler = {
        val dataset = it.query("dataset")!!
        val table = it.query("table")!!
        val currentOperationInfo = operationInfo[dataset]!![table]!!
        if (!(currentOperationInfo.preparingBulk)) {
            val bulkResponse = doSFBulkStartQuery(dataset, table)
            try {
                File("/tmp/bulkstartQueryResponse").writeText(bulkResponse.toMessage())
                if (bulkResponse.status != Status.OK) throw IllegalStateException("Bad request: ${bulkResponse.bodyString()}")
                val responseObj = JsonParser.parseString(bulkResponse.bodyString()) as JsonObject
                currentOperationInfo.jobId = responseObj["id"].asString
                currentOperationInfo.preparingBulk = true
            } catch (e: Exception) {
                log.error { "Fail when starting bulk: " + e.stackTraceToString() }
                throw e
            }
        }
        val bulkJobStatusResponse = doSFBulkJobStatusQuery(currentOperationInfo.jobId)

        if (!bulkJobStatusResponse.status.successful) {
            log.error { "Bad response from bulk job status query: ${bulkJobStatusResponse.bodyString()}" }
            throw IllegalStateException("Bad response from bulk job status query: ${bulkJobStatusResponse.bodyString()}")
        }

        try {
            val responseObj = JsonParser.parseString(bulkJobStatusResponse.bodyString()) as JsonObject
            currentOperationInfo.jobId = responseObj["id"].asString
            if (responseObj["state"].asString == "JobComplete") {
                File("/tmp/jobRegisteredAsDoneByPerformBulk").writeText("yes")
                currentOperationInfo.jobComplete = true
            }
        } catch (e: Exception) {
            log.error { e.stackTraceToString() }
        }
        Response(Status.OK).body(bulkJobStatusResponse.bodyString())
    }

    val resetHandler: HttpHandler = {
        val dataset = it.query("dataset")!!
        val table = it.query("table")!!
        operationInfo[dataset]!![table] = OperationInfo()
        Response(Status.OK)
    }

    val storeExpectedCountHandler: HttpHandler = {
        val dataset = it.query("dataset")!!
        val table = it.query("table")!!
        val count = it.query("count")!!
        operationInfo[dataset]!![table]!!.expectedCount = count.toLong()
        Response(Status.OK)
    }

    val transferHandler: HttpHandler = {
        val dataset = it.query("dataset")!!
        val table = it.query("table")!!
        val currentOperationInfo = operationInfo[dataset]!![table]!!
        if (!currentOperationInfo.preparingBulk) {
            Response(Status.PRECONDITION_FAILED).body("No batch operation initiated")
        } else if (!currentOperationInfo.jobComplete) {
            // Check if job is done since last check
            val bulkJobStatusResponse = doSFBulkJobStatusQuery(currentOperationInfo.jobId)
            val responseObj = JsonParser.parseString(bulkJobStatusResponse.bodyString()) as JsonObject
            if (responseObj["state"].asString == "JobComplete") {
                currentOperationInfo.jobComplete = true
            }
        }
        if (!currentOperationInfo.jobComplete) {
            Response(Status.PRECONDITION_FAILED).body("Batch job is not finished - cannot do data transfer yet")
        } else if (currentOperationInfo.transferDone) {
            Response(Status.OK).body(currentOperationInfo.transferReport)
        } else if (currentOperationInfo.transfering) {
            Response(Status.ACCEPTED).body(currentOperationInfo.transferReport)
        } else {
            currentOperationInfo.transfering = true
            GlobalScope.launch {
                runTransferJob(dataset, table)
            }
            currentOperationInfo.transferReport = "Transfer of job ${currentOperationInfo.jobId} started for $dataset $table${
                if (!application.postToBigQuery || application.excludeTables.contains(table)) {
                    " (Will not actually post due to ${if (!application.postToBigQuery) "postToBigQuery flag false" else ""} " +
                        "${if (application.excludeTables
                                .contains(
                                    table,
                                )
                        ) {
                            "table set as excluded"
                        } else {
                            ""
                        }})"
                } else {
                    ""
                }
            }"
            Response(Status.ACCEPTED).body(currentOperationInfo.transferReport)
        }
    }

    fun runTransferJob(
        dataset: String,
        table: String,
    ) {
        val currentOperationInfo = operationInfo[dataset]!![table]!!
        log.info { "Starting bulk transfer from batch job ${currentOperationInfo.jobId} to $dataset $table" }
        val schema = application.mapDef[dataset]!![table]!!.schema
        val tableId = TableId.of(application.projectId, dataset, table)
        var locator: String? = null

        do {
            val response = doSFBulkJobResultQuery(currentOperationInfo.jobId, locator)
            val arrays = parseCSVToJsonArrays(response.bodyString())

            val fragmentsSize = arrays.size
            val totalCount = arrays.sumOf { it.size() }
            arrays.forEachIndexed { index, array ->
                try {
                    remapAndSendRecords(array, tableId, schema)
                    currentOperationInfo.processedRecords += array.size()
                    val reportRow = "Processed ${array.size()} records (${index + 1}/$fragmentsSize of current batch)"
                    log.info { reportRow }
                    currentOperationInfo.transferReport += "\n$reportRow"
                } catch (e: Exception) {
                    File("/tmp/exceptionAtTransferJob").writeText(e.toString() + "\n" + e.stackTraceToString())
                    log.error { "Exception at remapAndSendRecords in transfer job - ${e.message}" }
                    currentOperationInfo.transferReport += "\nFail in batch operation - ${e.message}"
                    currentOperationInfo.transferDone = true
                    throw RuntimeException("Fail in batch operation - ${e.message}")
                }
            }

            // Next locator
            locator = response.header("Sforce-Locator")
            if (locator == "null") locator = null
            val reportRow = "Finished with batch of $totalCount records"
            log.info { reportRow }
            currentOperationInfo.transferReport += "\n$reportRow"
            if (locator == null) {
                currentOperationInfo.transferReport += "\nDone (${currentOperationInfo.processedRecords} records processed)"
                log.info { "Done (${currentOperationInfo.processedRecords} records processed)" }
                currentOperationInfo.transferDone = true
            }
        } while (locator != null)
    }
}
