import com.google.gson.JsonObject
import com.google.gson.JsonParser
import no.nav.sf.nada.addDateRestriction
import no.nav.sf.nada.parseMapDef
import no.nav.sf.nada.predictQueriesForWork
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.LocalDate

class QueryParseTest {
    private val exampleMapDefJson =
        """
        {
            "dataset": {
                "table": {
                    "query": "SELECT Id FROM Event",
                    "schema": {
                        "Id": {
                            "name": "id",
                            "type": "STRING"
                        }
                    }
                },
                "table2": {
                    "query": "SELECT Id FROM Event WHERE Source='A'",
                    "schema": {
                        "Id": {
                            "name": "id",
                            "type": "STRING"
                        }
                    }
                }
            }
        }
        """.trimIndent()

    private val exampleMapDef = parseMapDef(JsonParser.parseString(exampleMapDefJson) as JsonObject)

    @Test
    fun `Construe salesforce query with date restriction`() {
        val useForLastModifiedDate = exampleMapDef["dataset"]!!["table"]!!.useForLastModifiedDate
        val withoutTimePart = exampleMapDef["dataset"]!!["table"]!!.withoutTimePart
        val query = exampleMapDef["dataset"]!!["table"]!!.query
        Assertions.assertEquals("SELECT Id FROM Event", query)

        val queryWithDateRestriction = query.addDateRestriction(LocalDate.parse("2000-01-01"), useForLastModifiedDate, withoutTimePart)
        Assertions.assertEquals(
            "SELECT Id FROM Event WHERE ((LastModifiedDate >= 2000-01-01T00:00:00Z AND LastModifiedDate < 2000-01-02T00:00:00Z))",
            queryWithDateRestriction,
        )

        val query2 = exampleMapDef["dataset"]!!["table2"]!!.query
        Assertions.assertEquals("SELECT Id FROM Event WHERE Source='A'", query2)

        val queryWithDateRestriction2 = query2.addDateRestriction(LocalDate.parse("2000-01-01"), useForLastModifiedDate, withoutTimePart)

        Assertions.assertEquals(
            "SELECT Id FROM Event WHERE Source='A' AND ((LastModifiedDate >= 2000-01-01T00:00:00Z AND LastModifiedDate < 2000-01-02T00:00:00Z))",
            queryWithDateRestriction2,
        )

        val queryWithDateRestriction3 =
            query2.addDateRestriction(
                LocalDate.parse("2000-01-01"),
                useForLastModifiedDate + "AnotherDateField",
                withoutTimePart,
            )

        Assertions.assertEquals(
            "SELECT Id FROM Event WHERE Source='A' AND ((LastModifiedDate >= 2000-01-01T00:00:00Z AND LastModifiedDate < 2000-01-02T00:00:00Z) OR (AnotherDateField >= 2000-01-01T00:00:00Z AND AnotherDateField < 2000-01-02T00:00:00Z))",
            queryWithDateRestriction3,
        )
    }
}
