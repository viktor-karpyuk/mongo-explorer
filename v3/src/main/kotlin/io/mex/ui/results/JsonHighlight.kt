package io.mex.ui.results

import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.SpanStyle
import androidx.compose.ui.text.buildAnnotatedString
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.withStyle
import io.mex.util.EjsonType
import io.mex.util.formatValue
import io.mex.util.isWrapper
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

internal fun colorFor(t: EjsonType): Color = when (t) {
    EjsonType.Number, EjsonType.Long, EjsonType.Decimal -> Color(0xFF60A5FA)
    EjsonType.Boolean -> Color(0xFFFBBF24)
    EjsonType.Null -> Color.Gray
    EjsonType.ObjectId -> Color(0xFF34D399)
    EjsonType.Date -> Color(0xFFA78BFA)
    EjsonType.Binary, EjsonType.Regex -> Color(0xFFF472B6)
    EjsonType.MinKey, EjsonType.MaxKey -> Color.DarkGray
    EjsonType.ArrayT, EjsonType.ObjectT -> Color.Gray
    else -> Color.Unspecified
}

private val STRING_COLOR = Color(0xFF34D399)

private class Palette(val key: Color, val punct: Color)

/** Pretty-printed EJSON with per-type colors; EJSON wrappers render in shell form (ObjectId(…), ISODate(…)). */
@Composable
fun HighlightedJson(raw: String, modifier: Modifier = Modifier) {
    val palette = Palette(
        key = MaterialTheme.colorScheme.onSurface,
        punct = MaterialTheme.colorScheme.onSurfaceVariant,
    )
    val text = remember(raw, palette.key, palette.punct) { highlightEjson(raw, palette) }
    Text(
        text,
        modifier = modifier,
        fontFamily = FontFamily.Monospace,
        style = MaterialTheme.typography.bodySmall,
    )
}

private fun highlightEjson(raw: String, palette: Palette): AnnotatedString {
    val parsed = runCatching { Json.parseToJsonElement(raw) }.getOrNull()
        ?: return AnnotatedString(raw)
    return buildAnnotatedString { appendElement(parsed, 0, palette) }
}

private fun androidx.compose.ui.text.AnnotatedString.Builder.colored(color: Color, s: String) {
    if (color == Color.Unspecified) append(s)
    else withStyle(SpanStyle(color = color)) { append(s) }
}

private fun androidx.compose.ui.text.AnnotatedString.Builder.appendElement(
    v: JsonElement,
    indent: Int,
    p: Palette,
) {
    val pad = "  ".repeat(indent + 1)
    when {
        v is JsonObject && isWrapper(v) -> {
            val f = formatValue(v)
            colored(colorFor(f.type), f.label)
        }
        v is JsonObject -> {
            if (v.isEmpty()) {
                colored(p.punct, "{}")
                return
            }
            colored(p.punct, "{\n")
            v.entries.forEachIndexed { i, (k, child) ->
                append(pad)
                colored(p.key, JsonPrimitive(k).toString())
                colored(p.punct, ": ")
                appendElement(child, indent + 1, p)
                if (i < v.size - 1) colored(p.punct, ",")
                append("\n")
            }
            append("  ".repeat(indent))
            colored(p.punct, "}")
        }
        v is JsonArray -> {
            if (v.isEmpty()) {
                colored(p.punct, "[]")
                return
            }
            colored(p.punct, "[\n")
            v.forEachIndexed { i, child ->
                append(pad)
                appendElement(child, indent + 1, p)
                if (i < v.size - 1) colored(p.punct, ",")
                append("\n")
            }
            append("  ".repeat(indent))
            colored(p.punct, "]")
        }
        v is JsonNull -> colored(colorFor(EjsonType.Null), "null")
        v is JsonPrimitive -> when {
            v.isString -> colored(STRING_COLOR, v.toString())
            v.content == "true" || v.content == "false" -> colored(colorFor(EjsonType.Boolean), v.content)
            else -> colored(colorFor(EjsonType.Number), v.content)
        }
    }
}
