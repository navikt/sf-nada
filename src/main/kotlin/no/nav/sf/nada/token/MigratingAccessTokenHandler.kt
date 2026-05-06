package no.nav.sf.nada.token

import mu.KotlinLogging

class MigratingAccessTokenHandler(
    val new: NewAccessTokenHandler,
) : AccessTokenHandler {
    private val log = KotlinLogging.logger {}

    fun testAccess(): String =
        try {
            if (new.testAccess()) {
                "Retrieved access token via new (migrated)"
            } else {
                "Unknown state"
            }
        } catch (e: Exception) {
            if (AccessTokenHandlerLegacy.testAccess()) {
                "Retrieved access token via old (not yet migrated)"
            } else {
                "Unknown state, last exception message:" + e.message
            }
            // Will give exception if old do not work either
        }

    override val accessToken: String
        get() {
            return try {
                val token = new.accessToken
                log.info { "Retrieved access token via new" }
                token
            } catch (e: Throwable) {
                log.warn(e) { "New token handler failed, falling back to old" }
                val token = AccessTokenHandlerLegacy.accessToken
                log.info { "Retrieved access token via old" }
                token
            }
        }
    override val instanceUrl: String
        get() {
            return try {
                val token = new.instanceUrl
                log.info { "Retrieved instance url via new" }
                token
            } catch (e: Throwable) {
                log.warn(e) { "New token handler failed, falling back to old" }
                val token = AccessTokenHandlerLegacy.instanceUrl
                log.info { "Retrieved instance url via old" }
                token
            }
        }
    override val tenantId get() = "N/A"
}
