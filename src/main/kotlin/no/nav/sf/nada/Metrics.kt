package no.nav.sf.nada

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.exporter.common.TextFormat
import io.prometheus.client.hotspot.DefaultExports
import mu.KotlinLogging
import org.http4k.core.HttpHandler
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.StringWriter

object Metrics {
    private val log = KotlinLogging.logger { }

    val fetchRequest: Counter = registerCounter("fetch_request")

    val productsRead: Counter = registerLabelCounter("products_read", "table")

    val productsSent: Counter = registerLabelCounter("products_sent", "table")

    val aggregatesSuccessful: Counter = registerLabelCounter("aggregates_successful", "table")

    val productsQueryFailed: Counter = registerLabelCounter("products_query_failed", "table")

    val latestTotalFromTestCall: Gauge = registerLabelGauge("latest_total_from_test", "table")

    fun registerCounter(name: String): Counter =
        Counter
            .build()
            .name(name)
            .help(name)
            .register()

    fun registerLabelCounter(
        name: String,
        label: String,
    ): Counter =
        Counter
            .build()
            .name(name)
            .help(name)
            .labelNames(label)
            .register()

    fun registerGauge(name: String): Gauge =
        Gauge
            .build()
            .name(name)
            .help(name)
            .register()

    fun registerLabelGauge(
        name: String,
        label: String,
    ): Gauge =
        Gauge
            .build()
            .name(name)
            .help(name)
            .labelNames(label)
            .register()

    init {
        DefaultExports.initialize()
        log.info { "Prometheus metrics are ready" }
    }

    val metricsHandler: HttpHandler = {
        try {
            val str = StringWriter()
            TextFormat.write004(str, CollectorRegistry.defaultRegistry.metricFamilySamples())
            val result = str.toString()
            if (result.isEmpty()) {
                Response(Status.NO_CONTENT)
            } else {
                Response(Status.OK).body(result)
            }
        } catch (e: Exception) {
            log.error { "/prometheus failed writing metrics - ${e.message}" }
            Response(Status.INTERNAL_SERVER_ERROR)
        }
    }
}

// some metrics for Salesforce client
data class SFMetrics(
    val responseLatency: Histogram =
        Histogram
            .build()
            .name("sf_response_latency_seconds_histogram")
            .help("Salesforce response latency since last restart")
            .register(),
    val failedAccessTokenRequest: Gauge =
        Gauge
            .build()
            .name("sf_failed_access_token_request_gauge")
            .help("No. of failed access token requests to Salesforce since last restart")
            .register(),
    val accessTokenRefresh: Gauge =
        Gauge
            .build()
            .name("sf_access_token_refresh_gauge")
            .help("No. of required access token refresh to Salesforce since last restart")
            .register(),
) {
    fun clear() {
        failedAccessTokenRequest.clear()
        accessTokenRefresh.clear()
    }
}
