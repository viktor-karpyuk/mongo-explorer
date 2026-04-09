package com.kubrik.mex.model;

public record ConnectionState(
        String connectionId,
        Status status,
        String serverVersion,
        String lastError
) {
    public enum Status { DISCONNECTED, CONNECTING, CONNECTED, ERROR }

    public static ConnectionState disconnected(String id) {
        return new ConnectionState(id, Status.DISCONNECTED, null, null);
    }
}
