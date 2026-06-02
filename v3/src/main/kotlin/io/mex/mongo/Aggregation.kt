package io.mex.mongo

val STAGE_OPERATORS = listOf(
    "\$match", "\$project", "\$group", "\$sort", "\$limit", "\$skip", "\$unwind",
    "\$lookup", "\$count", "\$addFields", "\$set", "\$unset", "\$replaceRoot",
    "\$facet", "\$bucket", "\$bucketAuto", "\$sortByCount", "\$sample", "\$merge",
    "\$out", "\$redact", "\$geoNear", "\$graphLookup", "\$changeStream",
)

data class StageDraft(
    val id: String = java.util.UUID.randomUUID().toString(),
    var operator: String = "\$match",
    var body: String = "{}",
    var enabled: Boolean = true,
)

data class AggTemplate(
    val name: String,
    val description: String,
    val stages: List<Pair<String, String>>,
)

val TEMPLATES = listOf(
    AggTemplate("Total count", "How many documents match the filter?",
        listOf("\$count" to "\"total\"")),
    AggTemplate("Top-K by field", "Most frequent values of a field.", listOf(
        "\$group" to "{ _id: \"\$field\", n: { \$sum: 1 } }",
        "\$sort" to "{ n: -1 }",
        "\$limit" to "10",
    )),
    AggTemplate("Latest N documents", "Most recent by createdAt.", listOf(
        "\$sort" to "{ createdAt: -1 }",
        "\$limit" to "10",
    )),
    AggTemplate("Daily date histogram", "Count per day for createdAt.", listOf(
        "\$group" to "{ _id: { \$dateToString: { format: \"%Y-%m-%d\", date: \"\$createdAt\" } }, n: { \$sum: 1 } }",
        "\$sort" to "{ _id: 1 }",
    )),
    AggTemplate("Average per category", "Avg of value grouped by category.", listOf(
        "\$group" to "{ _id: \"\$category\", avg: { \$avg: \"\$value\" }, n: { \$sum: 1 } }",
        "\$sort" to "{ avg: -1 }",
    )),
    AggTemplate("Lookup join", "Join with another collection by foreign key.", listOf(
        "\$lookup" to "{ from: \"other\", localField: \"otherId\", foreignField: \"_id\", as: \"joined\" }",
    )),
    AggTemplate("Distinct count", "Cardinality of a field.", listOf(
        "\$group" to "{ _id: \"\$field\" }",
        "\$count" to "\"distinct\"",
    )),
    AggTemplate("Faceted breakdown", "Run several aggregations in parallel.", listOf(
        "\$facet" to "{ byStatus: [{ \$sortByCount: \"\$status\" }], total: [{ \$count: \"n\" }] }",
    )),
    AggTemplate("Project + reshape", "Pick fields and rename.", listOf(
        "\$project" to "{ _id: 0, name: 1, email: 1, fullName: { \$concat: [\"\$first\", \" \", \"\$last\"] } }",
    )),
    AggTemplate("Unwind array", "Flatten an array field into multiple docs.", listOf(
        "\$unwind" to "\"\$tags\"",
        "\$sortByCount" to "\"\$tags\"",
    )),
)
