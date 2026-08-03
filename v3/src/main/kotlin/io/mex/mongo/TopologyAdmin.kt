package io.mex.mongo

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import org.bson.Document
import java.util.concurrent.TimeUnit

/** `sh.startBalancer()` / `sh.stopBalancer()` — reversible, but pausing stops chunk migrations. */
fun setBalancer(client: MongoClient, enabled: Boolean) {
    client.getDatabase("admin").runCommand(Document(if (enabled) "balancerStart" else "balancerStop", 1))
}

/**
 * `rs.freeze(seconds)` — keeps a secondary from seeking election for [seconds]; 0 lifts
 * an existing freeze. The command only works on the member itself, and our client talks
 * to the set as a whole, so a short-lived direct client carries it there.
 */
fun freezeMember(baseUri: String, host: String, seconds: Int) {
    val settings = MongoClientSettings.builder()
        .applicationName("mongo-explorer-v3")
        .applyConnectionString(ConnectionString(directNodeUri(baseUri, host)))
        .applyToClusterSettings { it.serverSelectionTimeout(8, TimeUnit.SECONDS) }
        .applyToSocketSettings { it.connectTimeout(8, TimeUnit.SECONDS) }
        .build()
    MongoClients.create(settings).use { c ->
        c.getDatabase("admin").runCommand(Document("replSetFreeze", seconds))
    }
}
