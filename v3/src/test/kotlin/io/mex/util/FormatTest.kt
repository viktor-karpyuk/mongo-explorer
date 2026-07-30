package io.mex.util

import io.mex.ui.query.parseFindBody
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class FormatDurationTest {
    @Test
    fun `renders seconds`() = assertEquals("45s", formatDuration(45))

    @Test
    fun `renders minutes and seconds`() = assertEquals("3m 20s", formatDuration(200))

    @Test
    fun `renders hours and minutes`() = assertEquals("2h 5m", formatDuration(7_500))

    @Test
    fun `renders days and hours`() = assertEquals("1d 1h", formatDuration(90_000))

    @Test
    fun `clamps negatives rather than rendering nonsense`() = assertEquals("0s", formatDuration(-5))
}

class FormatBytesTest {
    @Test
    fun `renders raw bytes below a kilobyte`() = assertEquals("512 B", formatBytes(512))

    @Test
    fun `renders one decimal under ten units`() = assertEquals("1.5 KB", formatBytes(1_536))

    @Test
    fun `drops the decimal at ten units and above`() = assertEquals("10 KB", formatBytes(10_240))

    @Test
    fun `renders a negative size as unknown`() = assertEquals("—", formatBytes(-1))
}

class ParseFindBodyTest {
    @Test
    fun `parses a recorded find`() {
        val body = """{"filter":"{a:1}","projection":"","sort":"{b:-1}","skip":10,"limit":25}"""
        val parsed = parseFindBody(body)
        assertEquals("{a:1}", parsed?.filter)
        assertEquals("{b:-1}", parsed?.sort)
        assertEquals(10, parsed?.skip)
        assertEquals(25, parsed?.limit)
    }

    @Test
    fun `returns null when the filter is missing`() {
        assertNull(parseFindBody("""{"sort":"{}"}"""))
    }

    @Test
    fun `returns null on malformed json rather than throwing`() {
        assertNull(parseFindBody("not json"))
    }
}
