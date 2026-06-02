package io.mex

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
) : AutoCloseable {
    override fun close() = store.close()

    companion object {
        fun bootstrap(): AppContext {
            val store = Store.open(dbPath())
            val migrations = MigrationJobsRepo(store).also { it.reconcileOrphans() }
            return AppContext(
                store = store,
                connections = ConnectionsRepo(store),
                uriHistory = UriHistoryRepo(store),
                prefs = PrefsRepo(store),
                queryHistory = QueryHistoryRepo(store),
                migrations = migrations,
            )
        }

        private fun dbPath(): Path {
            val home = System.getProperty("user.home")
            val base = when {
                System.getProperty("os.name").startsWith("Mac", ignoreCase = true) ->
                    Paths.get(home, "Library", "Application Support", "MongoExplorerV3")
                System.getProperty("os.name").startsWith("Windows", ignoreCase = true) ->
                    Paths.get(System.getenv("APPDATA") ?: home, "MongoExplorerV3")
                else -> Paths.get(home, ".local", "share", "mex-v3")
            }
            return base.resolve("mex-v3.db")
        }
    }
}
