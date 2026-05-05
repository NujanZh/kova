package org.nur.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StorageEngineTest {

    private StorageEngine storage;

    @BeforeEach
    void setUp() {
        storage = new StorageEngine();
    }

    @Test
    void setAndGetReturnsValue() {
        storage.set("key", "value");
        assertEquals("value", storage.get("key"));
    }

    @Test
    void getMissingKeyReturnsNull() {
        assertNull(storage.get("missing"));
    }

    @Test
    void setOverwritesExistingValue() {
        storage.set("key", "first");
        storage.set("key", "second");
        assertEquals("second", storage.get("key"));
    }

    @Test
    void setOverwritesClearsExpiry() {
        long pastExpiry = System.currentTimeMillis() - 1000;
        storage.set("key", "original");
        storage.expire("key", pastExpiry);

        storage.set("key", "refreshed");
        assertEquals("refreshed", storage.get("key"));
    }

    @Test
    void setAndGetEmptyString() {
        storage.set("key", "");
        assertEquals("", storage.get("key"));
    }

    @Test
    void deleteExistingKeyReturnsTrue() {
        storage.set("key", "value");
        assertTrue(storage.delete("key"));
    }

    @Test
    void deleteRemovesKey() {
        storage.set("key", "value");
        storage.delete("key");
        assertNull(storage.get("key"));
    }

    @Test
    void deleteMissingKeyReturnsFalse() {
        assertFalse(storage.delete("missing"));
    }

    @Test
    void deleteAlreadyExpiredKeyReturnsFalse() {
        storage.set("key", "value");
        long pastExpiry = System.currentTimeMillis() - 1000;
        storage.expire("key", pastExpiry);

        assertFalse(storage.delete("key"));
    }

    @Test
    void existsReturnsTrueForPresentKey() {
        storage.set("key", "value");
        assertTrue(storage.exists("key"));
    }

    @Test
    void existsReturnsFalseForMissingKey() {
        assertFalse(storage.exists("missing"));
    }

    @Test
    void existsReturnsFalseForExpiredKey() {
        storage.set("key", "value");
        long pastExpiry = System.currentTimeMillis() - 1000;
        storage.expire("key", pastExpiry);

        assertFalse(storage.exists("key"));
    }

    @Test
    void expireOnExistingKeyReturnsTrue() {
        storage.set("key", "value");
        long futureExpiry = System.currentTimeMillis() + 10_000;
        assertTrue(storage.expire("key", futureExpiry));
    }

    @Test
    void expireOnMissingKeyReturnsFalse() {
        long futureExpiry = System.currentTimeMillis() + 10_000;
        assertFalse(storage.expire("missing", futureExpiry));
    }

    @Test
    void expireWithPastEpochDeletesKey() {
        storage.set("key", "value");
        long pastExpiry = System.currentTimeMillis() - 1000;
        storage.expire("key", pastExpiry);

        assertNull(storage.get("key"));
    }

    @Test
    void expireWithPastEpochReturnsTrueWhenKeyExisted() {
        storage.set("key", "value");
        long pastExpiry = System.currentTimeMillis() - 1000;
        assertTrue(storage.expire("key", pastExpiry));
    }

    @Test
    void expireWithFutureEpochKeyStillAccessible() {
        storage.set("key", "value");
        long futureExpiry = System.currentTimeMillis() + 10_000;
        storage.expire("key", futureExpiry);

        assertEquals("value", storage.get("key"));
    }

    @Test
    void expireOnAlreadyExpiredKeyReturnsFalse() {
        storage.set("key", "value");
        long pastExpiry = System.currentTimeMillis() - 2000;
        storage.expire("key", pastExpiry);

        long futureExpiry = System.currentTimeMillis() + 10_000;
        assertFalse(storage.expire("key", futureExpiry));
    }

    @Test
    void ttlReturnsMinus2ForMissingKey() {
        assertEquals(-2, storage.getTtl("missing"));
    }

    @Test
    void ttlReturnsMinus1ForKeyWithNoExpiry() {
        storage.set("key", "value");
        assertEquals(-1, storage.getTtl("key"));
    }

    @Test
    void ttlReturnsMinus2ForExpiredKey() {
        storage.set("key", "value");
        long pastExpiry = System.currentTimeMillis() - 1000;
        storage.expire("key", pastExpiry);

        assertEquals(-2, storage.getTtl("key"));
    }

    @Test
    void ttlReturnsRemainingSecondsForFutureExpiry() {
        storage.set("key", "value");
        long futureExpiry = System.currentTimeMillis() + 5_000;
        storage.expire("key", futureExpiry);

        long ttl = storage.getTtl("key");
        assertTrue(ttl >= 4 && ttl <= 5, "Expected TTL ~5s, got: " + ttl);
    }

    @Test
    void expiredKeyEvictedOnGet() {
        storage.set("key", "value");
        long pastExpiry = System.currentTimeMillis() - 1;
        storage.expire("key", pastExpiry);

        assertNull(storage.get("key"));
    }

    @Test
    void expiredKeyEvictedOnExists() {
        storage.set("key", "value");
        storage.expire("key", System.currentTimeMillis() - 1);

        assertFalse(storage.exists("key"));
    }

    @Test
    void expiredKeyNotReturnedAfterLaterSet() {
        storage.set("key", "value");
        storage.expire("key", System.currentTimeMillis() - 1);

        assertNull(storage.get("key"));

        storage.set("key", "fresh");
        assertEquals("fresh", storage.get("key"));
    }

    @Test
    void multipleKeysAreIndependent() {
        storage.set("a", "1");
        storage.set("b", "2");
        storage.set("c", "3");

        assertEquals("1", storage.get("a"));
        assertEquals("2", storage.get("b"));
        assertEquals("3", storage.get("c"));
    }

    @Test
    void expiringOneKeyDoesNotAffectOthers() {
        storage.set("a", "1");
        storage.set("b", "2");

        storage.expire("a", System.currentTimeMillis() - 1);

        assertNull(storage.get("a"));
        assertEquals("2", storage.get("b"));
    }

    @Test
    void deletingOneKeyDoesNotAffectOthers() {
        storage.set("a", "1");
        storage.set("b", "2");

        storage.delete("a");

        assertNull(storage.get("a"));
        assertEquals("2", storage.get("b"));
    }
}
