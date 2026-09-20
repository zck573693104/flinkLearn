package com.bigdata.lineage.parser;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 线程安全的 LRU SQL 血缘缓存（上限 1000 条）
 */
class SqlCache {
    
    private static final int MAX_ENTRIES = 1000;
    
    private final Map<String, Object> map = Collections.synchronizedMap(
            new LinkedHashMap<String, Object>(64, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
                    return size() > MAX_ENTRIES;
                }
            });
    
    @SuppressWarnings("unchecked")
    <T> T get(String key) {
        return (T) map.get(key);
    }
    
    void put(String key, Object value) {
        map.put(key, value);
    }
    
    void clear() {
        map.clear();
    }
    
    int size() {
        return map.size();
    }
}
