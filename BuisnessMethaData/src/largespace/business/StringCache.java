package largespace.business;

public class StringCache extends Cache<String> {
    @Override
    public String deduplicate(String s) {
        if (cache.containsKey(s)) {
            return cache.get(s);
        } else {
            // save memory by avoid value (=char[]) sharing
            String master = new String(s.toCharArray());
            cache.put(master, master);
            return master;
        }
    }
}
