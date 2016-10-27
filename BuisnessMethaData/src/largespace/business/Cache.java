package largespace.business;

import java.util.HashMap;
import java.util.Map;

public class Cache<T> {
    protected final Map<T, T> cache;

    public Cache() {
        this(1024);
    }

    public Cache(int initial) {
        cache = new HashMap<>(initial);
    }

    public T deduplicate(T o) {
        if (cache.containsKey(o)) {
            return cache.get(o);
        } else {
            cache.put(o, o);
            return o;
        }
    }

    public int size() {
        return cache.size();
    }
}
