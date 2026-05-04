package org.nur.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class StorageEngine {
    private record Entry(String value, long expiresAt) {
        public boolean isExpired() {
            return expiresAt > 0 && System.currentTimeMillis() > expiresAt;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(StorageEngine.class);

    private final ConcurrentHashMap<String, Entry> storage;

    public StorageEngine() {
        this.storage = new ConcurrentHashMap<>();
    }

    public void set(String key, String value) {
        storage.put(key, new Entry(value, 0));
        log.debug("SET '{}', storage size: {}", key, storage.size());
    }

    public String get(String key) {
        Entry entry = storage.get(key);
        if (entry == null) {
            log.debug("GET '{}' -> miss", key);
            return null;
        }
        if (entry.isExpired()) {
            storage.remove(key, entry);
            log.debug("GET '{}' -> expired, evicted lazily", key);
            return null;
        }

        log.debug("GET '{}' -> hit", key);
        return entry.value();
    }

    public long getTtl(String key) {
        Entry entry = storage.get(key);
        if (entry == null) {
            log.debug("TTL '{}' -> -2 (not found)", key);
            return -2;
        }
        if (entry.expiresAt == 0) {
            log.debug("TTL '{}' -> -1 (no expiry)", key);
            return -1;
        }
        if (entry.isExpired()) {
            log.debug("TTL '{}' -> -2 (expired)", key);
            return -2;
        }
        long ttl = (entry.expiresAt() - System.currentTimeMillis()) / 1000;
        log.debug("TTL '{}' -> {}s remaining", key, ttl);
        return ttl;
    }

    public boolean delete(String key) {
        Entry entry = storage.get(key);

        if (entry == null) {
            log.debug("DEL '{}' -> not found", key);
            return false;
        }

        if (entry.isExpired()) {
            storage.remove(key, entry);
            log.debug("DEL '{}' -> already expired, cleaned up", key);
            return false;
        }

        boolean removed = storage.remove(key, entry);
        log.debug("DEL '{}' -> {}", key, removed ? "deleted" : "lost race with another thread");
        return removed;
    }

    public boolean exists(String key) {
        boolean exists = get(key) != null;
        log.debug("EXISTS '{}' -> {}", key, exists);
        return exists;
    }

    public boolean expire(String key, long epochMillis) {
        if (epochMillis <= 0) {
            boolean removed = storage.remove(key) != null;
            log.debug("EXPIRE '{}' with past epoch, key removed: {}", key, removed);
            return removed;
        }

        boolean[] updated = {false};

        storage.computeIfPresent(
                key,
                (k, existing) -> {
                    if (existing.isExpired()) return existing;
                    updated[0] = true;
                    return new Entry(existing.value(), epochMillis);
                });

        log.debug(
                "EXPIRE '{}' at epoch ms {} -> {}",
                key,
                epochMillis,
                updated[0] ? "updated" : "key not found");
        return updated[0];
    }
}
