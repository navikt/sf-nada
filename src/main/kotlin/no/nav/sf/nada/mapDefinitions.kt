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
    val fieldDefMap: MutableMap<String, FieldDef>,
    val useForLastModifiedDate: List<String> = listOf("LastModifiedDate"),
    val withoutTimePart: Boolean = false,
    val mergeKeys: List<String> = listOf(),
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
            val objS = objT["schema"]!!.asJsonObject
            val useForLastModifiedDate =
                objT
                    .get("useForLastModifiedDate")
                    ?.asString
                    ?.split(",")
                    ?.map { it.trim() } ?: listOf("LastModifiedDate")
            // val withoutTimePart = objT.get("withoutTimePart")?.asBoolean ?: false
            val mergeKeys =
                objT
                    .get("mergeKeys")
                    ?.asString
                    ?.split(",")
                    ?.map { it.trim() } ?: listOf()

            val fieldDefMap: MutableMap<String, FieldDef> = mutableMapOf()

            objS.entrySet().forEach { fieldEntry ->
                val fieldDef = gson.fromJson(fieldEntry.value, FieldDef::class.java)
                fieldDefMap[fieldEntry.key] = fieldDef
            }

            val withoutTimePart = deriveWithoutTimePart(tableEntry.key, useForLastModifiedDate, fieldDefMap)

            result[dataSetEntry.key]!![tableEntry.key] =
                TableDef(
                    query = query,
                    fieldDefMap = fieldDefMap,
                    useForLastModifiedDate = useForLastModifiedDate,
                    withoutTimePart = withoutTimePart,
                    mergeKeys = mergeKeys,
                )
        }
    }
    return result
}

fun deriveWithoutTimePart(
    table: String,
    useForLastModifiedDate: List<String>,
    fieldDefMap: MutableMap<String, FieldDef>,
): Boolean {
    var result = "Derive withoutTimePart for $table: "
    var dateTime = false
    var date = false
    useForLastModifiedDate.forEach {
        if (it == "LastModifiedDate") {
            result += "LastModifiedDate means dateTime "
            dateTime = true
        } else {
            if (fieldDefMap.containsKey(it)) {
                if (fieldDefMap[it]!!.type == SupportedType.DATETIME) {
                    result += "$it - is dateTime "
                    dateTime = true
                } else if (fieldDefMap[it]!!.type == SupportedType.DATE) {
                    result += "$it - is date "
                    date = true
                } else {
                    result += "$it - is ${fieldDefMap[it]!!.type.name} NOT SUPPORTED "
                }
            } else {
                result += "$it - unknown time field!, guessing dateTime WARNING "
                dateTime = true
            }
        }
    }
    if (date && !dateTime) {
        result += " RESULT: withoutTimePart TRUE"
    } else if (dateTime && !date) {
        result += " RESULT: withoutTimePart FALSE"
    } else {
        result += " RESULT: CONFLICT!"
    }
    log.info(result)

    return (date && !dateTime)
}
