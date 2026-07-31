package io.mex

import io.mex.data.BackupsRepo
import io.mex.data.ConnectionsRepo
import io.mex.data.MigrationJobsRepo
import io.mex.data.PrefsRepo
import io.mex.data.QueryHistoryRepo
import io.mex.data.Store
import io.mex.data.UriHistoryRepo
import java.nio.file.Path
import java.nio.file.Paths

class AppContext(
    val store: Store,
    val connections: ConnectionsRepo,
    val uriHistory: UriHistoryRepo,
    val prefs: PrefsRepo,
    val queryHistory: QueryHistoryRepo,
    val migrations: MigrationJobsRepo,
    val backups: BackupsRepo,
    val dataDir: Path,
) : AutoCloseable {
    /** Managed location for mongodump output; every catalog row's path lives under it. */
    val backupsDir: Path get() = dataDir.resolve("backups")

    override fun close() = store.close()

    companion object {
        fun bootstrap(): AppContext {
            val dir = dataDir()
            val store = Store.open(dir.resolve("mex-v3.db"))
            val migrations = MigrationJobsRepo(store).also { it.reconcileOrphans() }
            val backups = BackupsRepo(store).also { it.reconcileOrphans() }
            return AppContext(
                store = store,
                connections = ConnectionsRepo(store),
                uriHistory = UriHistoryRepo(store),
                prefs = PrefsRepo(store),
                queryHistory = QueryHistoryRepo(store),
                migrations = migrations,
                backups = backups,
                dataDir = dir,
            )
        }

        private fun dataDir(): Path {
            val home = System.getProperty("user.home")
            return when {
                System.getProperty("os.name").startsWith("Mac", ignoreCase = true) ->
                    Paths.get(home, "Library", "Application Support", "MongoExplorerV3")
                System.getProperty("os.name").startsWith("Windows", ignoreCase = true) ->
                    Paths.get(System.getenv("APPDATA") ?: home, "MongoExplorerV3")
                else -> Paths.get(home, ".local", "share", "mex-v3")
            }
        }
    }
}
