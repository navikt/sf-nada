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
    val useForLastModifiedDate: List<Pair<String, SupportedType>> = listOf(Pair("LastModifiedDate", SupportedType.DATETIME)),
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

            val useForLastModifiedDate =
                objT
                    .get("useForLastModifiedDate")
                    ?.asString
                    ?.split(",")
                    ?.map { it.trim() }
                    ?.map { Pair(it, fieldDefMap[it]?.type ?: SupportedType.DATETIME) }
                    ?: listOf(Pair("LastModifiedDate", SupportedType.DATETIME))

            result[dataSetEntry.key]!![tableEntry.key] =
                TableDef(
                    query = query,
                    fieldDefMap = fieldDefMap,
                    useForLastModifiedDate = useForLastModifiedDate,
                    mergeKeys = mergeKeys,
                )
        }
    }
    return result
}
