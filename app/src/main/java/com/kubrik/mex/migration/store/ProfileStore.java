package com.kubrik.mex.migration.store;

import com.kubrik.mex.migration.profile.ProfileCodec;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** CRUD over {@code migration_profiles}. A profile is a named, reusable {@link MigrationSpec}.
 *  See docs/mvp-functional-spec.md §7.3 for the state model (valid / unbound / invalid). */
public final class ProfileStore {

    public record Profile(String id, String name, MigrationKind kind, MigrationSpec spec,
                          Instant createdAt, Instant updatedAt) {}

    private final Database db;
    private final ProfileCodec codec;

    public ProfileStore(Database db, ProfileCodec codec) {
        this.db = db;
        this.codec = codec;
    }

    public Profile save(String name, MigrationSpec spec) {
        String id = UUID.randomUUID().toString();
        long now = Instant.now().toEpochMilli();
        String sql = """
            INSERT INTO migration_profiles (id, name, kind, spec_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, name);
            ps.setString(3, spec.kind().name());
            ps.setString(4, codec.toJson(spec));
            ps.setLong(5, now);
            ps.setLong(6, now);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("save profile failed: " + e.getMessage(), e);
        }
        return new Profile(id, name, spec.kind(), spec,
                Instant.ofEpochMilli(now), Instant.ofEpochMilli(now));
    }

    public void update(String id, String name, MigrationSpec spec) {
        long now = Instant.now().toEpochMilli();
        String sql = "UPDATE migration_profiles SET name = ?, kind = ?, spec_json = ?, updated_at = ? WHERE id = ?";
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, name);
            ps.setString(2, spec.kind().name());
            ps.setString(3, codec.toJson(spec));
            ps.setLong(4, now);
            ps.setString(5, id);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("update profile failed: " + e.getMessage(), e);
        }
    }

    public void delete(String id) {
        try (PreparedStatement ps = db.connection().prepareStatement("DELETE FROM migration_profiles WHERE id = ?")) {
            ps.setString(1, id);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("delete profile failed: " + e.getMessage(), e);
        }
    }

    public Optional<Profile> get(String id) {
        String sql = "SELECT * FROM migration_profiles WHERE id = ?";
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(map(rs)) : Optional.empty();
            }
        } catch (Exception e) {
            throw new RuntimeException("get profile failed: " + e.getMessage(), e);
        }
    }

    public List<Profile> list() {
        String sql = "SELECT * FROM migration_profiles ORDER BY LOWER(name)";
        try (PreparedStatement ps = db.connection().prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<Profile> out = new ArrayList<>();
            while (rs.next()) out.add(map(rs));
            return out;
        } catch (Exception e) {
            throw new RuntimeException("list profiles failed: " + e.getMessage(), e);
        }
    }

    private Profile map(ResultSet rs) throws Exception {
        return new Profile(
                rs.getString("id"),
                rs.getString("name"),
                MigrationKind.valueOf(rs.getString("kind")),
                codec.fromJson(rs.getString("spec_json")),
                Instant.ofEpochMilli(rs.getLong("created_at")),
                Instant.ofEpochMilli(rs.getLong("updated_at")));
    }
}
