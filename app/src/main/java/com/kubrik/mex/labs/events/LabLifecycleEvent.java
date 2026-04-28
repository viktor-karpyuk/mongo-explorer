package com.kubrik.mex.labs.events;

import com.kubrik.mex.labs.model.LabEvent;
import com.kubrik.mex.labs.model.LabStatus;

/**
 * v2.8.4 — Event-bus payload for Lab lifecycle transitions. The
 * UI's rollout viewer subscribes to this stream; the RunningLabsPane
 * uses it to refresh status pills without polling.
 */
public record LabLifecycleEvent(
        long labId,
        LabStatus status,
        LabEvent.Kind kind,
        String message,   // nullable
        long at
) {}
