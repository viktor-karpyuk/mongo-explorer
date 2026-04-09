package com.kubrik.mex.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class HistoryStore {

    public record Entry(long id, String connectionId, String dbName, String collName, String kind, String body, long createdAt) {}

    private final Database db;

    public HistoryStore(Database db) { this.db = db; }

    public void add(String connectionId, String dbName, String collName, String kind, String body) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "INSERT INTO query_history(connection_id,db_name,coll_name,kind,body,created_at) VALUES(?,?,?,?,?,?)")) {
            ps.setString(1, connectionId);
            ps.setString(2, dbName);
            ps.setString(3, collName);
            ps.setString(4, kind);
            ps.setString(5, body);
            ps.setLong(6, System.currentTimeMillis());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Entry> recent(String connectionId, int limit) {
        List<Entry> out = new ArrayList<>();
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT id,connection_id,db_name,coll_name,kind,body,created_at FROM query_history WHERE connection_id=? ORDER BY id DESC LIMIT ?")) {
            ps.setString(1, connectionId);
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Entry(
                            rs.getLong(1), rs.getString(2), rs.getString(3),
                            rs.getString(4), rs.getString(5), rs.getString(6), rs.getLong(7)));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return out;
    }
}
