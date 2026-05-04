package org.nur.storage;

import java.util.concurrent.ConcurrentHashMap;

public class StorageEngine {
    private record Entry(String value, long expiresAt) {
        public boolean isExpired() {
            return expiresAt > 0 && System.currentTimeMillis() > expiresAt;
        }
    }

    private final ConcurrentHashMap<String, Entry> storage;

    public StorageEngine() {
        this.storage = new ConcurrentHashMap<>();
    }

    public void set(String key, String value) {
        storage.put(key, new Entry(value, 0));
    }

    public String get(String key) {
        Entry entry = storage.get(key);
        if (entry == null) return null;
        if (entry.isExpired()) {
            storage.remove(key, entry);
            return null;
        }
        return entry.value();
    }

    public long getTtl(String key) {
        Entry entry = storage.get(key);
        if (entry == null) return -2;
        if (entry.expiresAt == 0) return -1;
        if (entry.isExpired()) return -2;
        return (entry.expiresAt() - System.currentTimeMillis()) / 1000;
    }

    public boolean delete(String key) {
        return storage.remove(key) != null;
    }

    public boolean exists(String key) {
        return get(key) != null;
    }

    public boolean expire(String key, long epochMillis) {
        if (epochMillis <= 0) {
            if (exists(key)) {
                storage.remove(key);
                return true;
            }
            return false;
        }

        boolean[] updated = {false};

        storage.computeIfPresent(
                key,
                (k, existing) -> {
                    updated[0] = true;
                    return new Entry(existing.value(), epochMillis);
                });

        return updated[0];
    }
}
