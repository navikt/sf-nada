@file:Suppress("ktlint:standard:filename", "ktlint:standard:property-naming")

package no.nav.sf.nada

const val config_MAPDEF_FILE = "MAPDEF_FILE"
const val config_POST_TO_BIGQUERY = "POST_TO_BIGQUERY"
const val config_EXCLUDE_TABLES = "EXCLUDE_TABLES"
const val config_SF_TOKENHOST = "SF_TOKENHOST"
const val config_SALESFORCE_VERSION = "SALESFORCE_VERSION"

const val env_GCP_TEAM_PROJECT_ID = "GCP_TEAM_PROJECT_ID"

// Salesforce required secrets
const val secret_SFClientID = "SFClientID"
const val secret_SFUsername = "SFUsername"

// Salesforce required secrets related to keystore for signed JWT
const val secret_keystoreJKSB64 = "keystoreJKSB64"
const val secret_KeystorePassword = "KeystorePassword"
const val secret_PrivateKeyAlias = "PrivateKeyAlias"
const val secret_PrivateKeyPassword = "PrivateKeyPassword"

// New access secrets/config:
const val config_SALESFORCE_API_VERSION = "SALESFORCE_API_VERSION"
const val config_SF_TOKEN_HOST = "SF_TOKEN_HOST"
const val config_SF_JWT_USERNAME = "SF_JWT_USERNAME"

const val secret_SF_JWT_CLIENT_ID = "SF_JWT_CLIENT_ID"
const val secret_SF_JWT_KEYSTORE_B64 = "SF_JWT_KEYSTORE_B64"
const val secret_SF_JWT_KEYSTORE_PASSWORD = "SF_JWT_KEYSTORE_PASSWORD"

const val secret_SF_VALIDATION_CLIENT_ID = "SF_VALIDATION_CLIENT_ID"

fun env(name: String): String = System.getenv(name) ?: throw NullPointerException("Missing env $name")

fun envAsBoolean(env: String): Boolean = System.getenv(env).trim().toBoolean()

fun envAsList(env: String): List<String> =
    System
        .getenv(env)
        .split(",")
        .map { it.trim() }
        .toList()
