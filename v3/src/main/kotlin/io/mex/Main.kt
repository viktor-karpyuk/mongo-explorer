package io.mex

import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.DpSize
import androidx.compose.ui.window.Window
import androidx.compose.ui.window.WindowState
import androidx.compose.ui.window.application
import io.mex.ui.App

fun main() = application {
    Window(
        onCloseRequest = ::exitApplication,
        title = "Mongo Explorer",
        state = WindowState(size = DpSize(1400.dp, 900.dp)),
    ) {
        App()
    }
}
