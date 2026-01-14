
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import no.nav.sf.nada.parseMapDef
import no.nav.sf.nada.toRowMap
import org.junit.jupiter.api.Test

const val MAPDEF_FILE_DEV = "/mapdef/team-dialog/dev.json"
const val MAPDEF_FILE_PROD = "/mapdef/team-dialog/prod.json"

const val RECORD_KUNNSKAP_FILE = "/record_kunnskapsartikler.json"
const val RECORD_CHAT_FILE = "/record_chat_mock.json"
const val RECORD_AGREEMENT_FILE = "/record_agreement_with_nulls.json"

const val RECORD_MOETER_MISSING = "/record_moeter_missing.json"

public class RecordTranslateTest {
    val mapDefDev = parseMapDef(MAPDEF_FILE_DEV)
    val mapDefProd = parseMapDef(MAPDEF_FILE_PROD)

    @Test
    fun test_translation_of_object_missing_element() {
        val recordObj = JsonParser.parseString(RecordTranslateTest::class.java.getResource(RECORD_MOETER_MISSING).readText()) as JsonObject
        val fieldDefMap = mapDefProd["arbeidsgiver_aktivitet"]!!["arbeidsgiver_moeter"]!!.fieldDefMap

        recordObj.toRowMap(fieldDefMap)
        // println("Expected field $missingFieldNames missing in record, total sum ($missingFieldWarning)")
    }

    /*  // TODO Once we have a stable model to test against, we can do a variant of the testing below to ensure mapdef file is sound etc.
    @Test
    fun test_translation_of_agreement_record_with_nulls() {
        val recordObj = JsonParser.parseString(Bootstrap::class.java.getResource(RECORD_AGREEMENT_FILE).readText()) as JsonObject
        val fieldDefMap = mapDefDev["aareg_avtaler"]!!["aareg_avtaler_for_tilgang"]!!.fieldDefMap

        val expected: Map<String, Any?> = mapOf(
            "avtalenummer" to "AVT-000096",
            "organisasjonsnummer" to "810825472",
            "virksomhet" to "Malmefjorden og ridabu regnskap11",
            "integrert_oppslag_api" to false,
            "uttrekk" to false,
            "web_oppslag" to false,
            "opprettet" to "2022-12-05T11:35:05.000",
            "status" to "Active",
            "databehandler_navn" to null,
            "databehandler_organisasjonsnummer" to null,
            "virksomhetskategori" to "Electricity Supervision",
            "hjemmel_beslutningsgrunnlag_formal" to "Hjemmel: Aa-registerforskriften § 10 bokstav e \nFormål: Tilsyn med elektriske anlegg og elektrisk utstyr \nBehandlingsgrunnlag: asfdasf",
            "hendelser" to false
        )
        assertEquals(expected, recordObj.toRowMap(fieldDefMap))
    }

    @Test
    fun test_translation_of_kunnskapsrecord() {
        val recordObj = JsonParser.parseString(Bootstrap::class.java.getResource(RECORD_KUNNSKAP_FILE).readText()) as JsonObject
        val fieldDefMap = mapDefDev["nks_knowledge"]!!["kunnskapsartikler"]!!.fieldDefMap

        val expected: Map<String, Any> = mapOf(
            "id" to "ka00E0000008NiGQAU",
            "artikkel_nummer" to 1000,
            "versjonsnummer" to 2,
            "tittel" to "Tiltakspenger - Tilleggsstønader",
            "publisert_dato" to "2022-01-10T06:57:08.000"
        )
        assertEquals(expected, recordObj.toRowMap(fieldDefMap))
    }

    @Test
    fun test_translation_of_chatrecord() {
        val recordObj = JsonParser.parseString(Bootstrap::class.java.getResource(RECORD_CHAT_FILE).readText()) as JsonObject
        val fieldDefMap = mapDefDev["nks_chat"]!!["chat"]!!.fieldDefMap

        val expected: Map<String, Any> = mapOf(
            "id" to "mock-id",
            "chat_queue" to "Chat Mock",
            "start" to "2022-01-10T06:56:25.000",
            "slutt" to "2022-01-10T06:57:08.000"
        )
        assertEquals(expected, recordObj.toRowMap(fieldDefMap))
    }

     */
}
