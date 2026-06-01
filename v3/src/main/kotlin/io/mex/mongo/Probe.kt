package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document

internal data class ServerInfo(
    val pingMs: Int,
    val version: String,
    val topology: String,
)

internal fun probeServer(client: MongoClient): ServerInfo {
    val admin = client.getDatabase("admin")
    val t0 = System.nanoTime()
    admin.runCommand(Document("ping", 1))
    val pingMs = ((System.nanoTime() - t0) / 1_000_000).toInt()

    val buildInfo = admin.runCommand(Document("buildInfo", 1))
    val hello = admin.runCommand(Document("hello", 1))

    val version = (buildInfo["version"] as? String) ?: "unknown"
    val topology = describeTopology(hello)
    return ServerInfo(pingMs, version, topology)
}

private fun describeTopology(hello: Document): String {
    if (hello["msg"] == "isdbgrid") return "sharded"
    val setName = hello["setName"] as? String
    if (setName != null) return "replica-set ($setName)"
    if (hello["isWritablePrimary"] == true || hello["ismaster"] == true) return "standalone"
    return "unknown"
}
