package com.kubrik.mex.migration;

import com.kubrik.mex.migration.schedule.ScheduleExpression;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ScheduleExpressionTest {

    @Test
    void parsesEveryVariants() {
        assertEquals(Duration.ofSeconds(30), ScheduleExpression.intervalOf("@every 30s"));
        assertEquals(Duration.ofMinutes(15), ScheduleExpression.intervalOf("@every 15m"));
        assertEquals(Duration.ofHours(2),    ScheduleExpression.intervalOf("@every 2h"));
        assertEquals(Duration.ofDays(7),     ScheduleExpression.intervalOf("@every 7d"));
    }

    @Test
    void parsesNamedShorthands() {
        assertEquals(Duration.ofHours(1), ScheduleExpression.intervalOf("@hourly"));
        assertEquals(Duration.ofDays(1),  ScheduleExpression.intervalOf("@daily"));
        assertEquals(Duration.ofDays(7),  ScheduleExpression.intervalOf("@weekly"));
    }

    @Test
    void rejectsUnknownSyntax() {
        assertThrows(IllegalArgumentException.class, () -> ScheduleExpression.intervalOf("0 * * * *"));
        assertThrows(IllegalArgumentException.class, () -> ScheduleExpression.intervalOf(""));
        assertThrows(IllegalArgumentException.class, () -> ScheduleExpression.intervalOf("@every 0m"));
        assertThrows(IllegalArgumentException.class, () -> ScheduleExpression.intervalOf("@every 5x"));
        assertThrows(IllegalArgumentException.class, () -> ScheduleExpression.intervalOf(null));
    }

    @Test
    void nextAfterAddsInterval() {
        long base = 1_700_000_000_000L;
        assertEquals(base + 60_000, ScheduleExpression.nextAfter("@every 1m", base));
        assertEquals(base + 3_600_000, ScheduleExpression.nextAfter("@hourly", base));
    }
}
