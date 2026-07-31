package io.mex.ui.state

import androidx.compose.runtime.mutableStateOf

sealed class Selection {
    object Welcome : Selection()
    object Migrations : Selection()
    object Backups : Selection()
    object Settings : Selection()
    data class ConnectionView(val connectionId: String) : Selection()
    data class Database(val connectionId: String, val db: String) : Selection()
    data class Collection(val connectionId: String, val db: String, val collection: String) : Selection()
}

class SelectionStore {
    private val _state = mutableStateOf<Selection>(Selection.Welcome)
    val current: Selection get() = _state.value
    fun select(s: Selection) { _state.value = s }
}
