package com.github.torkjel.rt.api.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Simple cache implementation. All entries have a fixed time to live.
 */
public class Cache<T, U> {

    private final Map<T, CacheEntry<U>> cache = Collections.synchronizedMap(new HashMap<>());

    private final long ttl;

    /**
     * @param ttl time to live for cached values.
     */
    public Cache(long ttl) {
        this.ttl = ttl;
    }

    public void update(T key, U value) {
        cache.put(key, new CacheEntry<>(System.currentTimeMillis() + ttl, value));
    }

    public Optional<U> get(T key) {
        CacheEntry<U> ce = cache.get(key);
        return ce != null && !ce.isExpired()
                ? Optional.of(ce.getValue())
                : Optional.empty();
    }

}

@Data
@AllArgsConstructor
class CacheEntry<T> {
    private final Long expires;
    private final T value;

    public boolean isExpired() {
        return expires < System.currentTimeMillis();
    }
}
