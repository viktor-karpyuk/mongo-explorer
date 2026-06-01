package io.mex.ui

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import io.mex.AppContext
import io.mex.mongo.MongoRegistry
import io.mex.ui.connections.ConnectionsView
import io.mex.ui.theme.MexTheme

@Composable
fun App(ctx: AppContext) {
    val registry = remember { MongoRegistry() }

    MexTheme {
        Surface(
            modifier = Modifier.fillMaxSize(),
            color = MaterialTheme.colorScheme.background,
        ) {
            ConnectionsView(ctx = ctx, registry = registry)
        }
    }
}
