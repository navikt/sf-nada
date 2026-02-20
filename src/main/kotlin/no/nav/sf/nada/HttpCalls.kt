package no.nav.sf.nada

import no.nav.sf.nada.token.AccessTokenHandler
import org.http4k.client.OkHttp
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.urlEncoded
import java.io.File

object HttpCalls {
    private val client = lazy { OkHttp() }

    fun doSFQuery(query: String) = doCallWithSFToken("${AccessTokenHandler.instanceUrl}${application.sfQueryBase}${query.urlEncoded()}")

    fun doCallWithSFToken(uri: String): Response {
        val request =
            Request(Method.GET, uri)
                .header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")
                .header("Content-Type", "application/json;charset=UTF-8")
        File("/tmp/queryToHappen").writeText(request.toMessage())
        val response = client.value(request)
        File("/tmp/responseThatHappend").writeText(response.toMessage())
        return response
    }

    fun queryToUseForBulkQuery(
        dataset: String,
        table: String,
    ): String {
        val useForLastModifiedDate = application.mapDef[dataset]!![table]!!.useForLastModifiedDate
        val withoutTimePart = application.mapDef[dataset]!![table]!!.withoutTimePart
        return application.mapDef[dataset]!![table]!!
            .query
            .addNotRecordsFromTodayRestriction(useForLastModifiedDate, withoutTimePart)
    }

    fun doSFBulkStartQuery(
        dataset: String,
        table: String,
    ): Response {
        val query = queryToUseForBulkQuery(dataset, table)
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
