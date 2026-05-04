package no.nav.sf.nada.token

interface AccessTokenHandler {
    val accessToken: String
    val instanceUrl: String
    val tenantId: String
}
