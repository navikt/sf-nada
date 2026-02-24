package no.nav.sf.nada

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.JobStatistics
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TableId
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.gson.JsonPrimitive
import mu.KotlinLogging
import no.nav.sf.nada.HttpCalls.doCallWithSFToken
import no.nav.sf.nada.HttpCalls.doSFQuery
import no.nav.sf.nada.bulk.BulkOperation
import no.nav.sf.nada.token.AccessTokenHandler
import org.http4k.core.Response
import java.io.File
import java.lang.IllegalStateException
import java.lang.RuntimeException
import java.lang.Thread.sleep
import java.time.LocalDate

private val log = KotlinLogging.logger {}

fun fetchAndSend(
    targetDate: LocalDate?,
    dataset: String,
    table: String,
) {
    if (targetDate == null) {
        log.warn { "No localDate for fetchAndSend specified - will fetch for dataset $dataset table $table without date constraints" }
    } else {
        log.info { "Will perform fetchAndSend for dataset $dataset table $table on localDate $targetDate" }
    }
    if (!application.mapDef.containsKey(dataset)) {
        throw RuntimeException("mapDef.json is missing a definition for dataset $dataset")
    } else if (application.mapDef[dataset]?.containsKey(table) == false) {
        throw RuntimeException("mapDef.json is missing a definition for table $table in dataset $dataset")
    }

    val timeSliceFields = application.mapDef[dataset]!![table]!!.timeSliceFields

    val query =
        application.mapDef[dataset]!![table]!!.query.let { q ->
            if (targetDate == null) q else q.addDateRestriction(targetDate, timeSliceFields)
        }
    log.info { "Will use query: $query" }

    val schema = application.mapDef[dataset]!![table]!!.schema

    val tableId = TableId.of(application.projectId, dataset, table)

    Metrics.fetchRequest.inc()
    var response = doSFQuery(query)

    if (response.status.code == 400) {
        Metrics.productsQueryFailed.labels(table).inc()
        log.error { "${response.status.code}:${response.bodyString()}" }
        throw IllegalStateException("${response.status.code}:${response.bodyString()}")
    }
    var obj: JsonObject
    try {
        obj = JsonParser.parseString(response.bodyString()) as JsonObject
        File("/tmp/latestParsedObject").writeText(obj.toString())
    } catch (e: Exception) {
        throw IllegalStateException(response.toMessage() + "\n" + e.message)
    }
    val totalSize = obj["totalSize"].asInt
    var done = obj["done"].asBoolean
    var nextRecordsUrl: String? = obj["nextRecordsUrl"]?.asString
    log.info { "QUERY RESULT overview - totalSize $totalSize, done $done, nextRecordsUrl $nextRecordsUrl" }
    Metrics.productsRead.labels(table).inc(totalSize.toDouble())

    if (tableId.isStaging()) {
        truncateTable(tableId)
    }

    if (totalSize > 0) {
        var records = obj["records"].asJsonArray
        remapAndSendRecords(records, tableId, schema)
        while (!done) {
            response = doCallWithSFToken("${AccessTokenHandler.instanceUrl}$nextRecordsUrl")
            obj = JsonParser.parseString(response.bodyString()) as JsonObject
            done = obj["done"].asBoolean
            nextRecordsUrl = obj["nextRecordsUrl"]?.asString
            log.info { "CONTINUATION RESULT overview - totalSize $totalSize, done $done, nextRecordsUrl $nextRecordsUrl" }
            records = obj["records"].asJsonArray
            remapAndSendRecords(records, tableId, schema)
        }
    }

    if (tableId.isStaging()) {
        val mergeKeys = application.mapDef[dataset]!![table]!!.mergeKeys
        mergeStagingIntoTargetWithRetry(
            tableId,
            keys = mergeKeys,
        )
    }

    if (tableId.table == "community-user-login-v2" && targetDate != null) {
        log.info(
            "Post streaming raw data for community-user-login-v2 ($totalSize records), will attempt inserting into daily and hourly aggregate tables",
        )
        makeDailyAggregate(targetDate)
        makeHourlyAggregate(targetDate)
    }
}

fun Response.parsedRecordsCount(): Int {
    val obj = JsonParser.parseString(this.bodyString()) as JsonObject
    return obj["totalSize"].asInt
}

fun JsonObject.findBottomElement(defKey: String): JsonElement {
    // 1) Try direct lookup first (flat JSON)
    this.get(defKey)?.let { return it }

    // 2) Fallback to nested lookup
    val subKeys = defKey.split(".")
    var current: JsonElement = this

    for (key in subKeys) {
        if (current !is JsonObject || !current.has(key)) {
            return JsonNull.INSTANCE
        }
        current = current.get(key)
    }
    return current
}

fun remapAndSendRecords(
    records: JsonArray,
    tableId: TableId,
    fieldDefMap: MutableMap<String, FieldDef>,
) {
    val builder = InsertAllRequest.newBuilder(tableId)
    records.forEach { record ->
        builder.addRow((record as JsonObject).toRowMap(fieldDefMap))
    }
    val insertAllRequest = builder.build()
    records.last().let { File("/tmp/latestRecord_${tableId.table}").writeText("$it") }
    insertAllRequest.rows.last().let { File("/tmp/latestRow_${tableId.table}").writeText("$it") }
    insertAllRequest.rows
        .joinToString(",\n") { "$it" }
        .let { File("/tmp/allRows_${tableId.table}").writeText(it) }
    if (application.postToBigQuery && !(application.excludeTables.any { it == tableId.table })) {
        val response = application.bigQueryService.insertAll(insertAllRequest)
        if (response.hasErrors()) {
            log.error { "Failure at insert: ${response.insertErrors}" }
            throw RuntimeException("Failure at insert: ${response.insertErrors}")
        } else {
            Metrics.productsSent.labels(tableId.table).inc(records.count().toDouble())
            log.info { "Rows (${records.count()}) successfully inserted into dataset ${tableId.dataset}, table ${tableId.table}" }
        }
    } else {
        File("/tmp/wouldHaveSent").writeText(insertAllRequest.toString())
        log.info { "Rows (${records.count()}) ready to post but will skip due to postToBigQuery flag set to false" }
    }
}

fun JsonObject.toRowMap(fieldDefMap: MutableMap<String, FieldDef>): MutableMap<String, Any?> {
    File("/tmp/translateFieldDef").writeText(gson.toJson(fieldDefMap))
    File("/tmp/translateObject").writeText(gson.toJson(this))
    File("/tmp/translateProcess").writeText("")
    val rowMap: MutableMap<String, Any?> = mutableMapOf()
    fieldDefMap.forEach { defEntry ->
        val element = this.findBottomElement(defEntry.key)
        val elementValueOrNull = if (element is JsonNull) JsonPrimitive("null") else element
        File("/tmp/translateProcess").appendText("${elementValueOrNull.asString} -> ${defEntry.value.name} (${defEntry.value.type})\n")
        rowMap[defEntry.value.name] =
            if (element is JsonNull) {
                null
            } else {
                when (defEntry.value.type) {
                    SupportedType.STRING -> element.asString
                    SupportedType.INTEGER -> element.asInt
                    SupportedType.DATETIME -> element.asString.subSequence(0, 23)
                    SupportedType.DATE -> element.asString
                    SupportedType.BOOLEAN -> element.asBoolean
                }
            }
    }
    return rowMap
}

internal fun work(targetDate: LocalDate = LocalDate.now().minusDays(1)) {
    BulkOperation.initOperationInfo(application.mapDef) // Clear operation state for bulk jobs via gui
    log.info {
        "Work session starting to fetch for $targetDate excluding ${application.excludeTables} - post to BQ: ${application.postToBigQuery}"
    }
    try {
        application.mapDef.keys.forEach { dataset ->
            application.mapDef[dataset]!!
                .keys
                .filter {
                    !(application.excludeTables.contains(it)).also { excluding ->
                        if (excluding) log.info { "Will skip excluded table $it" }
                    }
                }.forEach { table ->
                    log.info { "Will attempt fetch and send for dataset $dataset, table $table, date $targetDate" }
                    fetchAndSend(targetDate, dataset, table)
                    application.hasPostedToday = true
                }
        }
    } catch (e: Exception) {
        log.error { "Failed to do work ${e.message} - has posted partially: ${application.hasPostedToday}" }
    }
    log.info { "Work session finished" }
}

fun predictQueriesForWork(targetDate: LocalDate = LocalDate.now().minusDays(1)): String {
    var result = "Target date: $targetDate\n"
    application.mapDef.keys.forEach { dataset ->
        application.mapDef[dataset]!!
            .keys
            .filter {
                !(application.excludeTables.contains(it)).also { excluding ->
                    if (excluding) result += " Will skip excluded table $it\n"
                }
            }.forEach { table ->
                val timeSliceFields = application.mapDef[dataset]!![table]!!.timeSliceFields
                val query =
                    application.mapDef[dataset]!![table]!!.query.addDateRestriction(targetDate, timeSliceFields)

                val bulk = HttpCalls.queryToUseForBulkQuery(dataset, table)

                result += "$dataset $table fetch query:\n$query\nbulk:\n $bulk\n**********************\n"

                application.hasPostedToday = true
            }
    }
    return result
}

fun truncateTable(tableId: TableId) {
    val bigQuery = application.bigQueryService

    val tableRef = "`${tableId.project}.${tableId.dataset}.${tableId.table}`"

    val query = "TRUNCATE TABLE $tableRef"

    val job =
        bigQuery.create(
            JobInfo.of(
                QueryJobConfiguration.newBuilder(query).build(),
            ),
        )

    job.waitFor()

    if (job.status.error != null) {
        throw RuntimeException("Failed to truncate table ${tableId.table}: ${job.status.error}")
    }
}

fun mergeStagingIntoTargetWithRetry(
    staging: TableId,
    keys: List<String>,
    maxRetries: Int = 10,
    initialDelayMs: Long = 2000,
) {
    val bigQuery = application.bigQueryService

    val stagingRef = "`${staging.project}.${staging.dataset}.${staging.table}`"
    val targetRef = "`${staging.project}.${staging.dataset}.${staging.stagingTarget()}`"

    val onClause = keys.joinToString(" AND ") { "T.$it = S.$it" }

    // Fetch target table schema to generate UPDATE / INSERT

    val targetTable =
        application.bigQueryService.getTable(
            TableId.of(staging.project, staging.dataset, staging.stagingTarget()),
            BigQuery.TableOption.fields(
                BigQuery.TableField.SCHEMA,
                BigQuery.TableField.NUM_ROWS,
            ),
        ) ?: throw RuntimeException("Target table not found")

    val definition = targetTable.getDefinition<TableDefinition>() as StandardTableDefinition

    val numRowsBefore = definition.numRows!!
    log.info("State of ${targetTable.tableId.table} before merge, numRows: $numRowsBefore")

    val columns = definition.schema!!.fields.map { it.name }

    // Exclude the merge keys from the update list (optional)
    val updateColumns = columns.filter { it !in keys }

    // Build UPDATE clause
    val updateClause = updateColumns.joinToString(", ") { col -> "T.$col = S.$col" }

    // Build INSERT clause
    val insertColumns = columns.joinToString(", ")
    val insertValues = columns.joinToString(", ") { col -> "S.$col" }

    val query =
        """
        MERGE $targetRef T
        USING $stagingRef S
        ON $onClause
        WHEN MATCHED THEN
          UPDATE SET $updateClause
        WHEN NOT MATCHED THEN
          INSERT ($insertColumns) VALUES ($insertValues)
        """.trimIndent()

    File("/tmp/latestMergeQuery").writeText(query)

    var attempt = 0
    var delay = initialDelayMs

    while (attempt < maxRetries) {
        try {
            val job =
                bigQuery.create(
                    JobInfo.of(QueryJobConfiguration.newBuilder(query).build()),
                )
            job.waitFor()

            if (job.status.error != null) {
                throw RuntimeException("Merge failed: ${job.status.error}")
            }

            val stats = job.getStatistics<JobStatistics.QueryStatistics>()

            log.info("Merge to ${staging.stagingTarget()} on unique keys $keys successful")

            // Success
            return
        } catch (e: BigQueryException) {
            val msg = e.message ?: ""
            if (msg.contains("streaming buffer") || msg.contains("would affect rows in the streaming buffer")) {
                attempt++
                println("MERGE blocked by streaming buffer, retry #$attempt in $delay ms...")
                sleep(delay)
                delay *= 2 // exponential backoff
            } else {
                throw e
            }
        }
    }

    throw RuntimeException("Merge failed after $maxRetries retries due to streaming buffer")
}

fun makeDailyAggregate(targetDate: LocalDate) {
    val bigQuery = application.bigQueryService

    // Format date for BigQuery
    val dateStr = targetDate.toString() // e.g., "2026-02-18"

    // Daily aggregation query with "ALL" network row
    val query =
        """
        INSERT INTO `platforce-prod-296b.license.community-user-login-daily`
        (date, networkId, networkName, logins, uniqueUsers)

        WITH per_network AS (
            SELECT
                DATE(loginAt) AS date,
                networkId,
                COUNT(id) AS logins,
                COUNT(DISTINCT userId) AS uniqueUsers
            FROM `platforce-prod-296b.license.community-user-login-v2`
            WHERE DATE(loginAt) = '$dateStr'
            GROUP BY networkId, DATE(loginAt)
        ),
        all_network AS (
            SELECT
                DATE(loginAt) AS date,
                'ALL' AS networkId,
                COUNT(id) AS logins,
                COUNT(DISTINCT userId) AS uniqueUsers
            FROM `platforce-prod-296b.license.community-user-login-v2`
            WHERE DATE(loginAt) = '$dateStr'
            GROUP BY DATE(loginAt)
        ),
        combined AS (
            SELECT * FROM per_network
            UNION ALL
            SELECT * FROM all_network
        )

        SELECT
            date,
            networkId,
            CASE networkId
                WHEN '0DB2o000000PCSHGA4' THEN 'Tolketjenesten'
                WHEN '0DB2o000000Ug64GAC' THEN 'Kurs'
                WHEN '0DB2o000000Ug69GAC' THEN 'nks'
                WHEN '0DB2o000000Ug6iGAC' THEN 'Aa-registeret'
                WHEN '0DB2o000000Ug9DGAS' THEN 'Innboks'
                WHEN '0DB2o000000Ug9IGAS' THEN 'Jobbsporet'
                WHEN '0DB7U0000004C9mWAE' THEN 'lesehjelp'
                WHEN '0DB7U0000004C9rWAE' THEN 'lesehjelpAura'
                WHEN '0DB7U0000004C9wWAE' THEN 'Kontaktskjema'
                WHEN '0DB7U0000008OIUWA2' THEN 'Tilbakemelding'
                WHEN '0DB7U0000008OIZWA2' THEN 'TilbakemeldingAura'
                WHEN '0DB7U000000TN3uWAG' THEN 'Arbeidsgiver Dialog'
                WHEN '0DB7U000000fxSzWAI' THEN 'innholdsbibliotek'
                WHEN 'ALL' THEN 'Sum'
                ELSE 'UKJENT'
            END AS networkName,
            logins,
            uniqueUsers
        FROM combined
        """.trimIndent()

    val job =
        bigQuery.create(
            JobInfo.of(QueryJobConfiguration.newBuilder(query).build()),
        )
    job.waitFor()

    if (job.status.error != null) {
        throw RuntimeException("Daily aggregate failed: ${job.status.error}")
    }
    log.info("Daily aggregate successful")
}

fun makeHourlyAggregate(targetDate: LocalDate) {
    val bigQuery = application.bigQueryService

    val dateStr = targetDate.toString()

    // Hourly aggregation query (login count per network per hour)
    val query =
        """
        INSERT INTO `platforce-prod-296b.license.community-user-login-hourly`
        (date, networkId, networkName, hour, logins)

        WITH combined AS (
            SELECT
                DATE(loginAt) AS date,
                networkId,
                EXTRACT(HOUR FROM loginAt) AS hour,
                COUNT(id) AS logins
            FROM `platforce-prod-296b.license.community-user-login-v2`
            WHERE DATE(loginAt) = '$dateStr'
            GROUP BY networkId, DATE(loginAt), hour

            UNION ALL

            SELECT
                DATE(loginAt) AS date,
                'ALL' AS networkId,
                EXTRACT(HOUR FROM loginAt) AS hour,
                COUNT(id) AS logins
            FROM `platforce-prod-296b.license.community-user-login-v2`
            WHERE DATE(loginAt) = '$dateStr'
            GROUP BY DATE(loginAt), hour
        )

        SELECT
            date,
            networkId,
            CASE networkId
                WHEN '0DB2o000000PCSHGA4' THEN 'Tolketjenesten'
                WHEN '0DB2o000000Ug64GAC' THEN 'Kurs'
                WHEN '0DB2o000000Ug69GAC' THEN 'nks'
                WHEN '0DB2o000000Ug6iGAC' THEN 'Aa-registeret'
                WHEN '0DB2o000000Ug9DGAS' THEN 'Innboks'
                WHEN '0DB2o000000Ug9IGAS' THEN 'Jobbsporet'
                WHEN '0DB7U0000004C9mWAE' THEN 'lesehjelp'
                WHEN '0DB7U0000004C9rWAE' THEN 'lesehjelpAura'
                WHEN '0DB7U0000004C9wWAE' THEN 'Kontaktskjema'
                WHEN '0DB7U0000008OIUWA2' THEN 'Tilbakemelding'
                WHEN '0DB7U0000008OIZWA2' THEN 'TilbakemeldingAura'
                WHEN '0DB7U000000TN3uWAG' THEN 'Arbeidsgiver Dialog'
                WHEN '0DB7U000000fxSzWAI' THEN 'innholdsbibliotek'
                WHEN 'ALL' THEN 'Sum'
                ELSE 'UKJENT'
            END AS networkName,
            hour,
            logins
        FROM combined
        """.trimIndent()

    val job =
        bigQuery.create(
            JobInfo.of(QueryJobConfiguration.newBuilder(query).build()),
        )
    job.waitFor()

    if (job.status.error != null) {
        throw RuntimeException("Hourly aggregate failed: ${job.status.error}")
    }
    log.info("Hourly aggregate successful")
}
