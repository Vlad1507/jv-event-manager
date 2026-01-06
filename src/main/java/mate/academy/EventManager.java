package mate.academy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EventManager {
    private final List<EventListener> eventListeners = new CopyOnWriteArrayList<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    public void registerListener(EventListener listener) {
        eventListeners.add(listener);
    }

    public void deregisterListener(EventListener listener) {
        eventListeners.remove(listener);
    }

    public void notifyEvent(Event event) {
        List<CompletableFuture<Void>> completableFutures = eventListeners.stream()
                .map(listener -> CompletableFuture.runAsync(
                        () -> listener.onEvent(event), executor)).toList();
        CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]));
    }

    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
