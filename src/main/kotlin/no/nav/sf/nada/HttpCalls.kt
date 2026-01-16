package no.nav.sf.nada

import no.nav.sf.nada.token.AccessTokenHandler
import org.http4k.client.OkHttp
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import java.io.File
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

object HttpCalls {
    private val client = lazy { OkHttp() }

    fun doSFQuery(query: String): Response {
        val request =
            Request(Method.GET, "$query")
                .header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")
                .header("Content-Type", "application/json;charset=UTF-8")
        File("/tmp/queryToHappen").writeText(request.toMessage())
        val response = client.value(request)
        File("/tmp/responseThatHappend").writeText(response.toMessage())
        // At 400 should to
        return response
    }

    private fun String.urlDecoded() = URLDecoder.decode(this, StandardCharsets.UTF_8.toString())

    fun doSFBulkStartQuery(
        dataset: String,
        table: String,
    ): Response {
        val useForLastModifiedDate = application.mapDef[dataset]!![table]!!.useForLastModifiedDate
        val withoutTimePart = application.mapDef[dataset]!![table]!!.withoutTimePart
        val query =
            application.mapDef[dataset]!![table]!!
                .query
                .addNotRecordsFromTodayRestriction(useForLastModifiedDate, withoutTimePart)
                .urlDecoded()
        val request =
            Request(Method.POST, "${AccessTokenHandler.instanceUrl}/services/data/${env(config_SALESFORCE_VERSION)}/jobs/query")
                .header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")
                .header("Content-Type", "application/json;charset=UTF-8")
                .body(
                    """{
                "operation": "query",
                "query": "$query",
                "contentType": "CSV"
                  }""".trim(),
                )

        File("/tmp/bulkQueryToHappen").writeText(request.toMessage())
        val response = client.value(request)
        File("/tmp/bulkResponseThatHappend").writeText(response.toMessage())
        return response
    }

    fun doSFBulkJobStatusQuery(jobId: String): Response {
        val request =
            Request(Method.GET, "${AccessTokenHandler.instanceUrl}/services/data/${env(config_SALESFORCE_VERSION)}/jobs/query/$jobId")
                .header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")
                .header("Content-Type", "application/json;charset=UTF-8")
        File("/tmp/bulkJobStatusQueryToHappen").writeText(request.toMessage())
        val response = client.value(request)
        File("/tmp/bulkJobStatusResponseThatHappend").writeText(response.toMessage())
        return response
    }

    fun doSFBulkJobResultQuery(
        jobId: String,
        locator: String? = null,
    ): Response {
        val request =
            Request(
                Method.GET,
                "${AccessTokenHandler.instanceUrl}/services/data/${env(
                    config_SALESFORCE_VERSION,
                )}/jobs/query/$jobId/results${locator?.let{"?locator=$locator"} ?: ""}",
            ).header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")

        val response = client.value(request)
        File("/tmp/bulkJobResultResponse${locator?.let{"-$locator"} ?: ""}").writeText(response.toMessage())
        return response
    }
}
