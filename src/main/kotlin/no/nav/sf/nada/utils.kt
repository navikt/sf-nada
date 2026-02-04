@file:Suppress("ktlint:standard:filename")

package no.nav.sf.nada

import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import java.io.File
import java.io.StringReader
import java.time.LocalDate
import java.time.format.DateTimeFormatter

private val log = KotlinLogging.logger { }

val gson = Gson()

/**
 * conditionalWait
 * Interruptable wait function
 */
fun conditionalWait(ms: Long) =
    runBlocking {
        log.debug { "Will wait $ms ms" }

        val cr =
            launch {
                runCatching { delay(ms) }
                    .onSuccess { log.debug { "Waiting completed" } }
                    .onFailure { log.info { "Waiting interrupted" } }
            }

        tailrec suspend fun loop(): Unit =
            when {
                cr.isCompleted -> Unit
                ShutdownHook.isActive() -> cr.cancel()
                else -> {
                    delay(250L)
                    loop()
                }
            }

        loop()
        cr.join()
    }

object ShutdownHook {
    private val log = KotlinLogging.logger { }

    @Volatile
    private var shutdownhookActiveOrOther = false
    private val mainThread: Thread = Thread.currentThread()

    init {
        log.info { "Installing shutdown hook" }
        Runtime
            .getRuntime()
            .addShutdownHook(
                object : Thread() {
                    override fun run() {
                        shutdownhookActiveOrOther = true
                        log.info { "shutdown hook activated" }
                        mainThread.join()
                    }
                },
            )
    }

    fun isActive() = shutdownhookActiveOrOther

    fun reset() {
        shutdownhookActiveOrOther = false
    }
}

fun String.addDateRestriction(
    localDate: LocalDate,
    dateFields: List<String>,
    withoutTimePart: Boolean,
): String {
    require(dateFields.isNotEmpty()) { "At least one date field must be provided" }

    val connector =
        if (contains(Regex("\\bWHERE\\b", RegexOption.IGNORE_CASE))) {
            " AND "
        } else {
            " WHERE "
        }

    fun format(date: LocalDate): String =
        buildString {
            append(date.format(DateTimeFormatter.ISO_DATE))
            if (!withoutTimePart) append("T00:00:00Z")
        }

    val today = format(localDate)
    val tomorrow = format(localDate.plusDays(1))

    val clause =
        dateFields.joinToString(" OR ") { field ->
            "($field >= $today AND $field < $tomorrow)"
        }

    return "$this$connector($clause)"
}

fun String.addHistoryLimitOnlyOneDateField(
    days: Int?,
    dateFields: List<String>,
): String {
    if (days == null) return this
    require(dateFields.isNotEmpty()) { "At least one date field must be provided" }

    val connector =
        if (contains(Regex("\\bWHERE\\b", RegexOption.IGNORE_CASE))) {
            " AND "
        } else {
            " WHERE "
        }

    val clause = "${dateFields.first()} = LAST_N_DAYS:$days"

    return "$this$connector($clause)"
}

fun String.addLimitRestriction(maxRecords: Int = 1000): String {
    val connector =
        if (this.contains("LIMIT", ignoreCase = true)) {
            throw IllegalArgumentException("Query already contains a LIMIT clause.")
        } else {
            " LIMIT"
        }
    return "$this$connector+$maxRecords"
}

fun String.addNotRecordsFromTodayRestriction(
    dateFields: List<String>,
    withoutTimePart: Boolean,
): String {
    if (dateFields.isEmpty()) return this

    val connector =
        if (contains(Regex("\\bWHERE\\b", RegexOption.IGNORE_CASE))) {
            " AND "
        } else {
            " WHERE "
        }

    val todayValue =
        buildString {
            append(LocalDate.now().format(DateTimeFormatter.ISO_DATE))
            if (!withoutTimePart) append("T00:00:00Z")
        }

    val andClause =
        dateFields.joinToString(" AND ") { field ->
            "$field<$todayValue"
        }

    return "$this$connector$andClause"
}

fun String.addYesterdayRestriction(
    useForLastModifiedDate: List<String>,
    withoutTimePart: Boolean,
): String = this.addDateRestriction(LocalDate.now().minusDays(1), useForLastModifiedDate, withoutTimePart)

fun parseCSVToJsonArrays(csvData: String): List<JsonArray> {
    File("/tmp/csvData").writeText(csvData)
    val listOfJsonArrays: MutableList<JsonArray> = mutableListOf()
    val rowLimit = 500
    var jsonArray = JsonArray()

    // Parse the CSV data with the new approach for headers
    val reader = StringReader(csvData)
    val csvParser =
        CSVParser(
            reader,
            CSVFormat.DEFAULT
                .builder()
                .setSkipHeaderRecord(true)
                .setHeader()
                .build(),
        )

    // Iterate through the records (skipping the header row)
    for (csvRecord in csvParser) {
        val jsonObject = JsonObject()

        // For each column in the record, add the key-value pair to the JsonObject
        csvRecord.toMap().forEach { (key, value) ->
            jsonObject.addProperty(key, if (value.isNullOrBlank()) null else value)
        }

        if (jsonArray.size() == rowLimit) {
            listOfJsonArrays.add(jsonArray)
            jsonArray = JsonArray()
        }
        // Add the JsonObject to the JsonArray
        jsonArray.add(jsonObject)
    }
    listOfJsonArrays.add(jsonArray)

    // Close the reader and parser
    csvParser.close()
    reader.close()

    File("/tmp/jsonArrayTypes").writeText(
        listOfJsonArrays
            .map { arr ->
                arr.map { it?.javaClass?.simpleName }
            }.joinToString(","),
    )

    return listOfJsonArrays
}
