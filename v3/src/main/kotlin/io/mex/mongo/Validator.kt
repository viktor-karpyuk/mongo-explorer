package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document

data class ValidatorPayload(
    val validator: String?,
    val validationLevel: String?,
    val validationAction: String?,
)

fun getValidator(client: MongoClient, db: String, coll: String): ValidatorPayload {
    val infos = client.getDatabase(db).listCollections().filter(Document("name", coll)).toList()
    if (infos.isEmpty()) return ValidatorPayload(null, null, null)
    val opts = (infos[0]["options"] as? Document) ?: Document()
    val v = opts["validator"] as? Document
    return ValidatorPayload(
        validator = v?.toJson(),
        validationLevel = opts.getString("validationLevel"),
        validationAction = opts.getString("validationAction"),
    )
}

fun setValidator(client: MongoClient, db: String, coll: String, p: ValidatorPayload) {
    val cmd = Document("collMod", coll)
    cmd["validator"] = p.validator?.takeIf { it.isNotBlank() }?.let { Document.parse(it) } ?: Document()
    p.validationLevel?.let { cmd["validationLevel"] = it }
    p.validationAction?.let { cmd["validationAction"] = it }
    client.getDatabase(db).runCommand(cmd)
}
