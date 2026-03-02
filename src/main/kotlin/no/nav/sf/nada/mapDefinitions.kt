@file:Suppress("ktlint:standard:filename")

package no.nav.sf.nada

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import mu.KotlinLogging

private val log = KotlinLogging.logger { }

data class FieldDef(
    val name: String,
    val type: SupportedType,
)

data class TableDef(
    val query: String,
    val schema: MutableMap<String, FieldDef>,
    val timeSliceFields: List<Pair<String, SupportedType>> = listOf(Pair("LastModifiedDate", SupportedType.DATETIME)),
    val mergeKeys: List<String> = listOf(),
    val aggregateSource: String? = null,
    val fullHistoryCopy: Boolean = false,
)

fun parseMapDef(filePath: String): Map<String, Map<String, TableDef>> =
    parseMapDef(JsonParser.parseString(Application::class.java.getResource(filePath)!!.readText()) as JsonObject)

fun parseMapDef(obj: JsonObject): Map<String, Map<String, TableDef>> {
    val result: MutableMap<String, MutableMap<String, TableDef>> = mutableMapOf()

    obj.entrySet().forEach { dataSetEntry ->
        val objDS = dataSetEntry.value.asJsonObject
        result[dataSetEntry.key] = mutableMapOf()
        objDS.entrySet().forEach { tableEntry ->
            val objT = tableEntry.value.asJsonObject
            val query = objT["query"]!!.asString
            val objS = objT["schema"]?.asJsonObject

            val mergeKeys =
                objT
                    .get("mergeKeys")
                    ?.asString
                    ?.split(",")
                    ?.map { it.trim() } ?: listOf()

            val aggregateSource = objT["aggregateSource"]?.asString
            val fullHistoryCopy = objT["fullHistoryCopy"]?.asBoolean ?: false

            val schema: MutableMap<String, FieldDef> = mutableMapOf()

            objS?.entrySet()?.forEach { fieldEntry ->
                val fieldDef = gson.fromJson(fieldEntry.value, FieldDef::class.java)
                schema[fieldEntry.key] = fieldDef
            }

            val timeSliceFields =
                objT
                    .get("timeSliceFields")
                    ?.asString
                    ?.split(",")
                    ?.map { it.trim() }
                    ?.map { Pair(it, schema[it]?.type ?: SupportedType.DATETIME) }
                    ?: listOf(Pair("LastModifiedDate", SupportedType.DATETIME))

            result[dataSetEntry.key]!![tableEntry.key] =
                TableDef(
                    query = query,
                    schema = schema,
                    timeSliceFields = timeSliceFields,
                    mergeKeys = mergeKeys,
                    aggregateSource = aggregateSource,
                    fullHistoryCopy = fullHistoryCopy,
                )
        }
    }
    return result
}
