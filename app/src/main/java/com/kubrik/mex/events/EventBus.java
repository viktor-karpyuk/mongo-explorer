package com.kubrik.mex.events;

import com.kubrik.mex.model.ConnectionState;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class EventBus {

    private final List<Consumer<ConnectionState>> stateListeners = new CopyOnWriteArrayList<>();
    private final List<BiConsumer<String, String>> logListeners = new CopyOnWriteArrayList<>();

    public void onState(Consumer<ConnectionState> l) { stateListeners.add(l); }
    public void onLog(BiConsumer<String, String> l) { logListeners.add(l); }

    public void publishState(ConnectionState s) {
        for (Consumer<ConnectionState> l : stateListeners) {
            try { l.accept(s); } catch (Exception ignored) {}
        }
    }

    public void publishLog(String connectionId, String line) {
        for (BiConsumer<String, String> l : logListeners) {
            try { l.accept(connectionId, line); } catch (Exception ignored) {}
        }
    }
}
