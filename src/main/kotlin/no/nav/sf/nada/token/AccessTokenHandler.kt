package no.nav.sf.nada.token

import com.google.gson.Gson
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.sf.nada.config_SF_TOKENHOST
import no.nav.sf.nada.env
import no.nav.sf.nada.secret_KeystorePassword
import no.nav.sf.nada.secret_PrivateKeyAlias
import no.nav.sf.nada.secret_PrivateKeyPassword
import no.nav.sf.nada.secret_SFClientID
import no.nav.sf.nada.secret_SFUsername
import no.nav.sf.nada.secret_keystoreJKSB64
import org.apache.commons.codec.binary.Base64.decodeBase64
import org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString
import org.http4k.client.OkHttp
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.body.toBody
import java.io.File
import java.security.KeyStore
import java.security.PrivateKey

/**
 * A handler for oauth2 access flow to salesforce.
 * @see [sf.remoteaccess_oauth_jwt_flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5)
 *
 * Fetches and caches access token, also retrieves instance url
 */
object AccessTokenHandler {
    val accessToken get() = fetchAccessTokenAndInstanceUrl().first
    val instanceUrl get() = fetchAccessTokenAndInstanceUrl().second

    fun refreshToken() {
        if ((expireTime - System.currentTimeMillis()) / 60000 < 30) { // Refresh if expireTime within 30 min
            log.info { "Refreshing access token" }
            accessToken
        }
    }

    private val log = KotlinLogging.logger { }

    private val SFTokenHost = env(config_SF_TOKENHOST)
    private val SFClientID = env(secret_SFClientID)
    private val SFUsername = env(secret_SFUsername)
    private val keystoreB64 = env(secret_keystoreJKSB64)
    private val keystorePassword = env(secret_KeystorePassword)
    private val privateKeyAlias = env(secret_PrivateKeyAlias)
    private val privateKeyPassword = env(secret_PrivateKeyPassword)

    private val client: Lazy<HttpHandler> = lazy { OkHttp() }

    private val gson = Gson()

    private const val EXP_TIME_SECONDS_CLAIM = 3600 // 60 min - expire time for the access token we ask salesforce for

    private var lastTokenPair = Pair("", "")

    private var expireTime = System.currentTimeMillis()

    private fun fetchAccessTokenAndInstanceUrl(): Pair<String, String> {
        if (System.currentTimeMillis() < expireTime) {
            log.debug { "Using cached access token (${(expireTime - System.currentTimeMillis()) / 60000} min left)" }
            return lastTokenPair
        }
        val expireMomentSinceEpochInSeconds = (System.currentTimeMillis() / 1000) + EXP_TIME_SECONDS_CLAIM
        val claim =
            JWTClaim(
                iss = SFClientID,
                aud = SFTokenHost,
                sub = SFUsername,
                exp = expireMomentSinceEpochInSeconds.toString(),
            )
        val privateKey =
            privateKeyFromBase64Store(
                ksB64 = keystoreB64,
                ksPwd = keystorePassword,
                pkAlias = privateKeyAlias,
                pkPwd = privateKeyPassword,
            )
        val claimWithHeaderJsonUrlSafe = "${
            gson.toJson(JWTClaimHeader("RS256")).encodeB64UrlSafe()
        }.${gson.toJson(claim).encodeB64UrlSafe()}"
        val fullClaimSignature = privateKey.sign(claimWithHeaderJsonUrlSafe.toByteArray())

        val accessTokenRequest =
            Request(Method.POST, "$SFTokenHost/services/oauth2/token")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(
                    listOf(
                        "grant_type" to "urn:ietf:params:oauth:grant-type:jwt-bearer",
                        "assertion" to "$claimWithHeaderJsonUrlSafe.$fullClaimSignature",
                    ).toBody(),
                )

        for (retry in 1..4) {
            try {
                val response: Response = client.value(accessTokenRequest)
                if (response.status.code == 200) {
                    File("/tmp/latestAccessTokenResponse").writeText(response.toMessage())
                    val accessTokenResponse = gson.fromJson(response.bodyString(), AccessTokenResponse::class.java)
                    lastTokenPair = Pair(accessTokenResponse.access_token, accessTokenResponse.instance_url)
                    expireTime = (expireMomentSinceEpochInSeconds - 10) * 1000
                    return lastTokenPair
                }
            } catch (e: Exception) {
                log.error("Attempt to fetch access token $retry of 3 failed by ${e.message}")
                runBlocking { delay(retry * 1000L) }
            }
        }
        log.error("Attempt to fetch access token given up")
        return Pair("", "")
    }

    private fun privateKeyFromBase64Store(
        ksB64: String,
        ksPwd: String,
        pkAlias: String,
        pkPwd: String,
    ): PrivateKey =
        KeyStore.getInstance("JKS").apply { load(ksB64.decodeB64().inputStream(), ksPwd.toCharArray()) }.run {
            getKey(pkAlias, pkPwd.toCharArray()) as PrivateKey
        }

    private fun PrivateKey.sign(data: ByteArray): String =
        this.let {
            java.security.Signature
                .getInstance("SHA256withRSA")
                .apply {
                    initSign(it)
                    update(data)
                }.run {
                    sign().encodeB64()
                }
        }

    private fun ByteArray.encodeB64(): String = encodeBase64URLSafeString(this)

    private fun String.decodeB64(): ByteArray = decodeBase64(this)

    private fun String.encodeB64UrlSafe(): String = encodeBase64URLSafeString(this.toByteArray())

    private data class JWTClaim(
        val iss: String,
        val aud: String,
        val sub: String,
        val exp: String,
    )

    private data class JWTClaimHeader(
        val alg: String,
    )

    private data class AccessTokenResponse(
        val access_token: String,
        val scope: String,
        val instance_url: String,
        val id: String,
        val token_type: String,
    )
}
