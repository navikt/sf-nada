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
import org.http4k.urlEncoded
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
    useForLastModifiedDate: String,
    withoutTimePart: Boolean,
): String {
    val connector = if (this.contains("WHERE")) "+AND" else "+WHERE"
    return this +
        "$connector+$useForLastModifiedDate>=TODAY+AND+$useForLastModifiedDate<=TOMORROW"
            .replace("TODAY", "${localDate.format(DateTimeFormatter.ISO_DATE)}${if (withoutTimePart) "" else "T00:00:00Z"}".urlEncoded())
            .replace(
                "TOMORROW",
                "${localDate.plusDays(1).format(DateTimeFormatter.ISO_DATE)}${if (withoutTimePart) "" else "T00:00:00Z"}".urlEncoded(),
            ).replace(">", ">".urlEncoded())
            .replace("<", "<".urlEncoded())
}

fun String.addHistoryLimit(
    days: Int?,
    useForLastModifiedDate: String,
): String {
    // If days == null, no restriction
    if (days == null) return this

    val connector = if (this.contains("WHERE", ignoreCase = true)) "+AND+" else "+WHERE+"

    return "$this$connector$useForLastModifiedDate=LAST_N_DAYS:$days"
}

fun String.addLimitRestriction(maxRecords: Int = 1000): String {
    val connector =
        if (this.contains("LIMIT", ignoreCase = true)) {
            throw IllegalArgumentException("Query already contains a LIMIT clause.")
        } else {
            "+LIMIT"
        }
    return "$this$connector+$maxRecords"
}

fun String.addNotRecordsFromTodayRestriction(
    useForLastModifiedDate: String,
    withoutTimePart: Boolean,
): String {
    val connector = if (this.contains("WHERE")) "+AND" else "+WHERE"
    return this +
        "$connector+$useForLastModifiedDate<TODAY"
            .replace(
                "TODAY",
                "${LocalDate.now().format(DateTimeFormatter.ISO_DATE)}${if (withoutTimePart) "" else "T00:00:00Z"}".urlEncoded(),
            ).replace("<", "<".urlEncoded())
}

fun String.addYesterdayRestriction(
    useForLastModifiedDate: String,
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
