package no.nav.sf.nada

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import mu.KotlinLogging
import no.nav.sf.nada.bulk.BulkOperation
import no.nav.sf.nada.gui.Gui
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.routing.ResourceLoader
import org.http4k.routing.bind
import org.http4k.routing.routes
import org.http4k.routing.static
import org.http4k.server.Netty
import org.http4k.server.asServer
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

private val resetRangeStart = LocalTime.parse("00:00:01")
private val resetRangeStop = LocalTime.parse("01:59:00")

private val log = KotlinLogging.logger { }

class Application {
    val projectId = env(env_GCP_TEAM_PROJECT_ID)

    val sfQueryBase = "/services/data/${env(config_SALESFORCE_VERSION)}/query?q="

    val mapDef = parseMapDef(env(config_MAPDEF_FILE))

    val postToBigQuery = envAsBoolean(config_POST_TO_BIGQUERY)

    val excludeTables = envAsList(config_EXCLUDE_TABLES)

    var hasPostedToday = true // Assume posted today Use oneOff below if you want to post for certain dates at deploy

    val bigQueryService: BigQuery =
        BigQueryOptions
            .newBuilder()
            .setProjectId(projectId)
            .build()
            .service

    private fun api(): HttpHandler =
        routes(
            "/internal/isAlive" bind Method.GET to { Response(Status.OK) },
            "/internal/isReady" bind Method.GET to { Response(Status.OK) },
            "/internal/metrics" bind Method.GET to Metrics.metricsHandler,
            "/internal/gui" bind Method.GET to static(ResourceLoader.Classpath("gui")),
            "/internal/metadata" bind Method.GET to Gui.metaDataHandler,
            "/internal/testSalesforceQuery" bind Method.GET to Gui.testCallHandler,
            "/internal/projectId" bind Method.GET to { Response(Status.OK).body(application.projectId) },
            "/internal/performBulk" bind Method.GET to BulkOperation.performBulkHandler,
            "/internal/transfer" bind Method.GET to BulkOperation.transferHandler,
            "/internal/reset" bind Method.GET to BulkOperation.resetHandler,
            "/internal/storeExpectedCount" bind Method.GET to BulkOperation.storeExpectedCountHandler,
            "/internal/predictQueries" bind Method.GET to { Response(Status.OK).body(predictQueriesForWork()) },
        )

    private fun apiServer(port: Int = 8080) = api().asServer(Netty(port))

    fun start() {
        BulkOperation.initOperationInfo(mapDef)
        log.info { "Starting app with settings: projectId $projectId, postTobigQuery $postToBigQuery, excludeTables $excludeTables" }
        apiServer().start()
        log.info { "1 minutes graceful start - establishing connections" }
        Thread.sleep(60000)

        /* // One offs (remember to remove after one run):
        oneOff("2024-05-23")
        oneOff("2024-05-30")
        oneOff("2024-06-03")
        oneOff("2024-06-07")
        oneOff("2024-06-10")
        oneOff("2024-06-13")
         */

        loop()

        log.info { "App Finished!" }
    }

    fun oneOff(localDateAsString: String) = work(LocalDate.parse(localDateAsString))

    private tailrec fun loop() {
        val stop = ShutdownHook.isActive()
        when {
            stop -> Unit
            !stop -> {
                if (hasPostedToday) {
                    if (LocalTime.now().inResetRange()) {
                        log.warn { "It is now a new day - set posted flag back to false" }
                        hasPostedToday = false
                    } else {
                        // log.info { "Has posted logs today - will sleep 30 minutes." }
                    }
                } else {
                    if (LocalTime.now().inActiveRange()) {
                        work()
                    } else {
                        log.info {
                            "Waiting for active range (later then ${resetRangeStop.format(
                                DateTimeFormatter.ISO_TIME,
                            )}) - will sleep 30 minutes."
                        }
                    }
                }
                conditionalWait(1800000) // Half an hour
                loop()
            }
        }
    }

    private fun LocalTime.inResetRange(): Boolean = this.isAfter(resetRangeStart) && this.isBefore(resetRangeStop)

    private fun LocalTime.inActiveRange(): Boolean = this.isAfter(resetRangeStop)
}
