package no.nav.sf.nada

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import mu.KotlinLogging
import no.nav.sf.nada.HttpCalls.doSFQuery
import no.nav.sf.nada.bulk.BulkOperation
import no.nav.sf.nada.gui.Gui
import no.nav.sf.nada.token.AccessTokenHandler
import no.nav.sf.nada.token.AccessTokenHandlerLegacy
import no.nav.sf.nada.token.MigratingAccessTokenHandler
import no.nav.sf.nada.token.NewAccessTokenHandler
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.core.Status.Companion.OK
import org.http4k.routing.ResourceLoader
import org.http4k.routing.bind
import org.http4k.routing.routes
import org.http4k.routing.static
import org.http4k.server.Netty
import org.http4k.server.asServer
import java.io.File
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

private val resetRangeStart = LocalTime.parse("00:00:01")
private val resetRangeStop = LocalTime.parse("01:29:00")

private val log = KotlinLogging.logger { }

class Application {
    val accessTokenHandler: AccessTokenHandler = MigratingAccessTokenHandler(new = NewAccessTokenHandler())

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
            "/internal/isAlive" bind Method.GET to { Response(OK) },
            "/internal/isReady" bind Method.GET to { Response(OK) },
            "/internal/metrics" bind Method.GET to Metrics.metricsHandler,
            "/internal/gui" bind Method.GET to static(ResourceLoader.Classpath("gui")),
            "/internal/metadata" bind Method.GET to Gui.metaDataHandler,
            "/internal/testSalesforceQuery" bind Method.GET to Gui.testCallHandler,
            "/internal/projectId" bind Method.GET to { Response(OK).body(application.projectId) },
            "/internal/performBulk" bind Method.GET to BulkOperation.performBulkHandler,
            "/internal/transfer" bind Method.GET to BulkOperation.transferHandler,
            "/internal/reset" bind Method.GET to BulkOperation.resetHandler,
            "/internal/storeExpectedCount" bind Method.GET to BulkOperation.storeExpectedCountHandler,
            "/internal/predictQueries" bind Method.GET to {
                Response(OK)
                    .header("Content-Type", "text/plain; charset=utf-8")
                    .body(predictQueriesForWork())
            },
            "/internal/testAccess/old" bind Method.GET to testAccessHandlerOld,
            "/internal/testAccess/new" bind Method.GET to testAccessHandlerNew,
            "/internal/testAccess/validation" bind Method.GET to testAccessHandlerValidation,
            "/internal/testAccess/migration" bind Method.GET to testAccessHandlerMigration,
            "/internal/files" bind Method.GET to filesHandler(File("/tmp/files")),
            "/internal/files/{path:.*}" bind Method.GET to filesHandler(File("/tmp/files")),
        )

    private fun apiServer(port: Int = 8080) = api().asServer(Netty(port))

    fun start() {
        BulkOperation.initOperationInfo(mapDef)
        log.info { "Starting app with settings: projectId $projectId, postTobigQuery $postToBigQuery, excludeTables $excludeTables" }
        apiServer().start()
        log.info { "1 minutes graceful start - establishing connections" }
        val dir = File("/tmp/files/subfolder")
        dir.mkdirs() // ensures /tmp/files exists

        File(dir, "testfile").writeText("Content of test file")
        Thread.sleep(60000)

//        val query3 =
//            "SELECT " +
//                "    NetworkId, " +
//                "    HOUR_IN_DAY(LoginTime), " +
//                "    COUNT(Id) " +
//                "FROM LoginHistory " +
//                "WHERE NetworkId != NULL " +
//                "  AND DAY_ONLY(LoginTime) = 2026-02-18 " +
//                "GROUP BY " +
//                "    NetworkId, " +
//                "    HOUR_IN_DAY(LoginTime)"
//
//        val result3 = doSFQuery(query3)
        // File("/tmp/resultOfInvestigate3").writeText(result3.toMessage())

        /* // One offs (remember to remove after one run), to run current day session (post yesterdays records) use work() or hasPostedToday = false:
        oneOff("2024-05-23")
        oneOff("2024-05-30")
        oneOff("2024-06-03")
        oneOff("2024-06-07")
        oneOff("2024-06-10")
        oneOff("2024-06-13")
        // fetchAndSend(LocalDate.now().minusDays(1), dataset, table) - use this to only do for one specific table (try out on a -staging table to test merge for example)
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

private val testAccessHandlerOld: HttpHandler = {
    Response(OK).body("$currentTimeStamp\nTest access (old) successful: " + AccessTokenHandlerLegacy.testAccess())
}

private val testAccessHandlerNew: HttpHandler = {
    val newAccessTokenHandler = NewAccessTokenHandler()
    Response(OK).body("$currentTimeStamp\nTest access (new) successful: " + newAccessTokenHandler.testAccess())
}

private val testAccessHandlerValidation: HttpHandler = {
    val newAccessTokenHandlerAgainstValidation = NewAccessTokenHandler(sfClientId = env(secret_SF_VALIDATION_CLIENT_ID))
    Response(OK).body("$currentTimeStamp\nTest access (validation) successful: " + newAccessTokenHandlerAgainstValidation.testAccess())
}

private val testAccessHandlerMigration: HttpHandler = {
    val migrationTokenHandler =
        MigratingAccessTokenHandler(new = NewAccessTokenHandler())
    Response(OK).body("$currentTimeStamp\nTest access (migration) result: " + migrationTokenHandler.testAccess())
}

private fun filesHandler(baseDir: File): HttpHandler =
    { request ->
        val path =
            request.uri.path
                .removePrefix("/internal/files")
                .trim('/')

        val target = if (path.isEmpty()) baseDir else File(baseDir, path)

        if (!target.exists()) {
            Response(Status.NOT_FOUND).body("Not found")
        } else if (target.isDirectory) {
            // List directory contents
            val files = target.listFiles()?.sortedBy { it.name } ?: emptyList()

            val html =
                buildString {
                    append(
                        """
        <html>
        <head>
            <link rel="stylesheet" href="/internal/gui/style.css">
        </head>
        <body>
            <div id="project-title">File Browser</div>

            <div class="dataset-section">
                <div class="dataset-header">Index of ${request.uri.path}</div>
    """,
                    )

                    if (path.isNotEmpty()) {
                        append(
                            """<div class="table">
            <a href="../" class="table-header">
                <div class="name-and-label-wrapper">../</div>
            </a>
        </div>""",
                        )
                    }

                    files.forEach { file ->
                        val name = file.name + if (file.isDirectory) "/" else ""
                        val link = "${request.uri.path.trimEnd('/')}/$name"

                        append(
                            """
            <div class="table">
                <a href="$link" class="table-header">
                    <div class="name-and-label-wrapper">
                       $name
                    </div>
                </a>
            </div>
        """,
                        )
                    }

                    append(
                        """
            </div>
        </body>
        </html>
    """,
                    )
                }

            Response(Status.OK)
                .header("Content-Type", "text/html; charset=utf-8")
                .body(html)
        } else {
            // Serve file content
            Response(Status.OK)
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(target.readText())
        }
    }
