package no.nav.sf.nada.gui

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.Table
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TableId
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.opencensus.stats.Aggregation
import mu.KotlinLogging
import no.nav.sf.nada.HttpCalls.doSFQuery
import no.nav.sf.nada.Metrics
import no.nav.sf.nada.SupportedType
import no.nav.sf.nada.TableDef
import no.nav.sf.nada.addHistoryLimitOnlyOneDateField
import no.nav.sf.nada.addYesterdayRestriction
import no.nav.sf.nada.application
import no.nav.sf.nada.bulk.BulkOperation
import no.nav.sf.nada.bulk.OperationInfo
import no.nav.sf.nada.gson
import no.nav.sf.nada.isStaging
import no.nav.sf.nada.stagingTarget
import no.nav.sf.nada.token.AccessTokenHandler
import org.http4k.core.HttpHandler
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.File

typealias BigQueryMetadata = Map<String, Gui.DatasetMetadata>

object Gui {
    private val log = KotlinLogging.logger { }

    val metaDataHandler: HttpHandler = {
        val response =
            try {
                Response(Status.OK).body(gson.toJson(fetchBigQueryMetadata(application.mapDef)))
            } catch (e: java.lang.Exception) {
                Response(Status.INTERNAL_SERVER_ERROR).body(e.stackTraceToString())
            }
        response
    }

    val testCallHandler: HttpHandler = { req: Request ->
        File("/tmp/latesttestcallrequest").writeText(req.toMessage())
        var result = ""
        val dataset = req.query("dataset")
        val table = req.query("table")
        log.info { "Will perform testcall $dataset $table" }
        val timeSliceFields = application.mapDef[dataset]!![table]!!.timeSliceFields
        val query = application.mapDef[dataset]!![table]!!.query

        val queryYesterday = query.addYesterdayRestriction(timeSliceFields)

        var success = true
        var yesterday = 0
        var last5 = 0

        val responseDate = doSFQuery(queryYesterday)

        File("/tmp/responseAtDateCall").writeText(responseDate.toMessage())
        if (responseDate.status.code == 400) {
            result += "\nBad request: " + responseDate.bodyString()
            File("/tmp/badRequestAtDateCall").writeText(queryYesterday + "\nRESULT:\n" + responseDate.bodyString())
            success = false
        } else {
            try {
                val obj = JsonParser.parseString(responseDate.bodyString()) as JsonObject
                val totalSize = obj["totalSize"].asInt
                result += "\nNumber of records from yesterday poll $totalSize"
                log.info { "Number of records from yesterday poll $totalSize" }
                yesterday = totalSize
            } catch (e: Exception) {
                success = false
                result += "\n" + e.message
                File("/tmp/exceptionAtDateCall").writeText(e.toString() + "\n" + e.stackTraceToString())
            }
        }
        File("/tmp/testcallResult").writeText(result)

        result += "<br>"

        val responseTotal = doSFQuery(query.addHistoryLimitOnlyOneDateField(5, timeSliceFields))

        File("/tmp/responseLast5Call").writeText(
            "Query: ${AccessTokenHandler.instanceUrl}${application.sfQueryBase}${query.addHistoryLimitOnlyOneDateField(
                5,
                timeSliceFields,
            )}\nRESPONSE:\n" +
                responseTotal.toMessage(),
        )
        when (responseTotal.status.code) {
            400 -> {
                result += "Bad request: " + responseTotal.bodyString()
                File("/tmp/badRequestLast5Call").writeText(responseTotal.bodyString())
                success = false
            }
            504 -> {
                result += "Gateway timeout: " + responseTotal.bodyString()
                File("/tmp/gatewayTimeoutLast5Call").writeText(responseTotal.bodyString())
                success = false
            }
            else -> {
                try {
                    val obj = JsonParser.parseString(responseTotal.bodyString()) as JsonObject
                    val totalSize = obj["totalSize"].asInt
                    Metrics.latestTotalFromTestCall.labels(table).set(totalSize.toDouble())
                    result += "Total number of records found is $totalSize"
                    log.info { "Total number of records found is $totalSize" }
                    last5 = totalSize
                } catch (e: Exception) {
                    success = false
                    result += e.message
                    File("/tmp/exceptionAtTotalCall").writeText(e.toString() + "\n" + e.stackTraceToString())
                }
            }
        }

        val response =
            if (success) {
                val pair = Pair(yesterday, last5)
                Response(Status.OK).body(gson.toJson(pair))
            } else {
                Response(Status.BAD_REQUEST).body(result)
            }

        File("/tmp/lasttestcallresponse").writeText(response.toMessage())
        response
    }

    // Data classes for metadata
    data class ColumnMetadata(
        val name: String,
        val type: String,
        val mode: String,
        val salesforceFieldName: String? = null,
    )

    data class TableMetadata(
        val tableName: String,
        val numRows: Long,
        val numRowsTarget: Long,
        val columns: List<ColumnMetadata>,
        val salesforceQuery: String? = null,
        val timeSliceFields: String = "LastModifiedDate",
        val mergeKeys: String = "",
        val aggregateSource: String = "",
        val fullHistoryCopy: Boolean = false,
        val active: Boolean = true,
        val operationInfo: OperationInfo,
    )

    data class DatasetMetadata(
        val tables: List<TableMetadata>,
    )

    private fun fetchBigQueryMetadata(mapDef: Map<String, Map<String, TableDef>>): BigQueryMetadata {
        val result = mutableMapOf<String, DatasetMetadata>()

        // List datasets in the project
        val datasets = application.bigQueryService.listDatasets(application.projectId).iterateAll()

        for (dataset in datasets) {
            val datasetName = dataset.datasetId.dataset

            // List of table metadata
            val tablesInfo = mutableListOf<TableMetadata>()

            // List tables in the dataset
            val tables = application.bigQueryService.listTables(datasetName).iterateAll()

            // Build a set of all table names for quick lookup
            val tableNames = tables.map { it.tableId.table }.toSet()

            for (table in tables) {
                val fullTable: Table
                var fullTableTarget: Table? = null
                var numRowsTarget = 0L
                try {
                    fullTable =
                        application.bigQueryService.getTable(
                            table.tableId,
                            BigQuery.TableOption.fields(BigQuery.TableField.NUM_ROWS, BigQuery.TableField.SCHEMA),
                        )

                    if (table.tableId.isStaging()) {
                        fullTableTarget =
                            application.bigQueryService.getTable(
                                TableId.of(table.tableId.project, table.tableId.dataset, table.tableId.stagingTarget()),
                                BigQuery.TableOption.fields(BigQuery.TableField.NUM_ROWS, BigQuery.TableField.SCHEMA),
                            )
                        val definitionTarget = fullTableTarget.getDefinition<TableDefinition>()
                        if (definitionTarget is StandardTableDefinition) {
                            numRowsTarget = definitionTarget.numRows!!
                        }
                    }
                } catch (e: Exception) {
                    log.error { e.printStackTrace() }
                    throw e
                }
                val definition = fullTable.getDefinition<TableDefinition>()

                // Skip tables that are not standard definitions (like views)
                if (definition is StandardTableDefinition) {
                    val tableName = table.tableId.table

                    // Skip base table if a staging version exists
                    if (!tableName.endsWith("-staging") &&
                        tableNames.contains("$tableName-staging")
                    ) {
                        log.info("Metadata for GUI: $tableName has a staging counterpart - will ignore")
                        continue
                    }

                    val tableQuery =
                        mapDef[datasetName]?.get(tableName)?.query?.replace("+", " ")
                            ?: "No query configured"

                    val timeSliceFields =
                        mapDef[datasetName]?.get(tableName)?.timeSliceFields ?: listOf(Pair("Missing", SupportedType.DATETIME))

                    val mergeKeys = mapDef[datasetName]?.get(tableName)?.mergeKeys ?: listOf()

                    val aggregateSource = mapDef[datasetName]?.get(tableName)?.aggregateSource ?: ""

                    val fullHistoryCopy = mapDef[datasetName]?.get(tableName)?.fullHistoryCopy ?: false

                    val selectFields = extractFields(tableQuery)

                    val numRows = definition.numRows!!
                    val bigQuerySchema = definition.schema!!

                    val columns = mutableListOf<ColumnMetadata>()
                    val configSchema = mapDef[datasetName]?.get(tableName)?.schema

                    for (field in bigQuerySchema.fields) {
                        val salesforceFieldName =
                            configSchema
                                ?.entries
                                ?.find { it.value.name == field.name } // Match BigQuery column name with fieldDefMap.name
                                ?.key
                                ?.let { if (selectFields.contains(it)) it else "$it - Missing in query" }
                                ?: "No mapping configured"

                        val typeText =
                            configSchema
                                ?.entries
                                ?.find { it.value.name == field.name } // Match BigQuery column name with fieldDefMap.name
                                ?.value
                                ?.type
                                ?.name
                                ?.let {
                                    if (it ==
                                        field.type.name()
                                    ) {
                                        it
                                    } else {
                                        "$it (configured) / ${field.type.name()} (Big Query) - Mismatch types"
                                    }
                                }
                                ?: "No mapping configured"

                        // log.info { "Looking up $salesforceFieldName for $field for $tableName" }
                        val columnInfo =
                            ColumnMetadata(
                                name = field.name,
                                type = typeText,
                                mode = field.mode?.name ?: "NULLABLE",
                                salesforceFieldName = salesforceFieldName, // Populate with Salesforce field name
                            )
                        columns.add(columnInfo)
                    }

                    // Looking up SF fields in SELECT that has no mapping - therfor no definition what corresponding BigQ name is
                    selectFields
                        .filter { selectField -> configSchema?.keys?.let { !it.contains(selectField) } ?: false }
                        .forEach { queryFieldUnmapped ->
                            val columnInfoQueryFieldNotMapped =
                                ColumnMetadata(
                                    name = "",
                                    type = "",
                                    mode = "",
                                    salesforceFieldName = "$queryFieldUnmapped - No mapping configured",
                                )
                            columns.add(columnInfoQueryFieldNotMapped)
                        }

                    selectFields
                        .filter { selectField ->
                            configSchema?.keys?.let { it.contains(selectField) } ?: false
                        } // Has map entry
                        .filter { selectField ->
                            configSchema?.entries?.find { it.key == selectField }?.value?.name.let { mappedBigQField ->
                                bigQuerySchema.fields.let { !(it.any { it.name == mappedBigQField }) }
                            }
                        }.forEach { queryFieldMappedToNonExistingBigQueryField ->
                            val columnInfoQueryFieldMappedToNonExistingBigQueryField =
                                ColumnMetadata(
                                    name =
                                        configSchema
                                            ?.entries
                                            ?.find { it.key == queryFieldMappedToNonExistingBigQueryField }
                                            ?.value
                                            ?.name!! +
                                            " - Not existing",
                                    type = "",
                                    mode = "",
                                    salesforceFieldName = queryFieldMappedToNonExistingBigQueryField, // Populate with Salesforce field name
                                )
                            columns.add(columnInfoQueryFieldMappedToNonExistingBigQueryField)
                        }

                    // Add table metadata to the list
                    val tableMetadata =
                        TableMetadata(
                            tableName = tableName,
                            numRows = numRows,
                            numRowsTarget = numRowsTarget,
                            columns = columns,
                            salesforceQuery = tableQuery,
                            timeSliceFields = timeSliceFields.joinToString(","),
                            mergeKeys = mergeKeys.joinToString(","),
                            aggregateSource = aggregateSource,
                            fullHistoryCopy = fullHistoryCopy,
                            active = application.postToBigQuery && !(application.excludeTables.any { it == tableName }),
                            operationInfo = BulkOperation.operationInfo[datasetName]?.get(tableName) ?: OperationInfo(),
                        )
                    tablesInfo.add(tableMetadata)
                } else {
                    log.info {
                        "Skipping bigquery $datasetName ${table.tableId.table} that has a definition of type ${definition.type.name()}"
                    }
                }
            }

            // Add dataset metadata to the result map
            val datasetMetadata = DatasetMetadata(tables = tablesInfo)
            result[datasetName] = datasetMetadata
        }

        return result
    }

    private fun extractFields(query: String): List<String> {
        val regex = Regex("SELECT\\s+(.*?)\\s+FROM", RegexOption.IGNORE_CASE)
        val matchResult = regex.find(query)
        return matchResult
            ?.groups
            ?.get(1)
            ?.value
            ?.split(",")
            ?.map { it.trim() } // Trim any extra spaces
            ?: emptyList()
    }
}
