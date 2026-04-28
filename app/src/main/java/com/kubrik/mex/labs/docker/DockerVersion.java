package com.kubrik.mex.labs.docker;

/**
 * v2.8.4 LAB-DOCKER-2 — Parsed Docker CLI version. Comparisons are
 * semver-lexical over (major, minor, patch). Only three segments —
 * Docker's version strings sometimes tack on a build suffix
 * ({@code 24.0.7-rd1}) we ignore.
 */
public record DockerVersion(int major, int minor, int patch) implements Comparable<DockerVersion> {

    /** Minimum Docker CLI version we ship against. 24.0 introduced
     *  the {@code --format json} support on {@code compose ls} that
     *  LabReconciler depends on; below that, the parser would have
     *  to fall back to table-mode scraping. */
    public static final DockerVersion MIN_SUPPORTED = new DockerVersion(24, 0, 0);

    public static DockerVersion parse(String versionOutput) {
        if (versionOutput == null) return null;
        // `docker --version` produces: "Docker version 24.0.7, build afdd53b"
        // `docker version --format '{{.Client.Version}}'` produces: "24.0.7"
        String line = versionOutput.trim().split("\\R", 2)[0];
        int i = 0;
        while (i < line.length() && !Character.isDigit(line.charAt(i))) i++;
        if (i >= line.length()) return null;
        int start = i;
        while (i < line.length()
                && (Character.isDigit(line.charAt(i)) || line.charAt(i) == '.')) i++;
        String[] parts = line.substring(start, i).split("\\.");
        try {
            int mj = parts.length > 0 ? Integer.parseInt(parts[0]) : 0;
            int mn = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            int pt = parts.length > 2 ? Integer.parseInt(parts[2]) : 0;
            return new DockerVersion(mj, mn, pt);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public boolean atLeast(DockerVersion other) {
        return this.compareTo(other) >= 0;
    }

    @Override
    public int compareTo(DockerVersion o) {
        int c = Integer.compare(this.major, o.major);
        if (c != 0) return c;
        c = Integer.compare(this.minor, o.minor);
        if (c != 0) return c;
        return Integer.compare(this.patch, o.patch);
    }

    public String asString() { return major + "." + minor + "." + patch; }
}
