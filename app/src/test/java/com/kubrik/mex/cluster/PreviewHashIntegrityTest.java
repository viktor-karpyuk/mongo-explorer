package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import com.kubrik.mex.cluster.safety.PreviewHashChecker;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PreviewHashIntegrityTest {

    @Test
    void untouchedBsonPasses() {
        DryRunResult preview = DryRunRenderer.render(new Command.StepDown("a:1", 60, 10));
        assertDoesNotThrow(() -> PreviewHashChecker.requireMatch(preview.commandBson(), preview.previewHash()));
    }

    @Test
    void mutatingBsonAfterPreviewIsRejected() {
        DryRunResult preview = DryRunRenderer.render(new Command.StepDown("a:1", 60, 10));
        Document mutated = new Document(preview.commandBson());
        mutated.put("replSetStepDown", 120);    // user-facing preview said 60
        PreviewHashChecker.PreviewTamperedException ex = assertThrows(
                PreviewHashChecker.PreviewTamperedException.class,
                () -> PreviewHashChecker.requireMatch(mutated, preview.previewHash()));
        assertEquals(preview.previewHash(), ex.expected);
        assertNotEquals(preview.previewHash(), ex.actual);
    }

    @Test
    void reorderedKeysStillMatch() {
        DryRunResult preview = DryRunRenderer.render(new Command.StepDown("a:1", 60, 10));
        Document reordered = new Document()
                .append("secondaryCatchUpPeriodSecs", 10)
                .append("replSetStepDown", 60);
        // Canonical JSON normalises key order; hash should match.
        assertDoesNotThrow(() ->
                PreviewHashChecker.requireMatch(reordered, preview.previewHash()));
    }
}
