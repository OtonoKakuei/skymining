package largespace.business;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public final class Utils {
    private Utils() {
    }

    public static String join(String[] s, String delimiter) {
        return join(Arrays.asList(s), delimiter);
    }

    public static <T, D> String join(Iterable<T> s, D delimiter) {
        if (s == null) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (T part : s) {
            if (first) {
                first = false;
            } else {
                builder.append(delimiter);
            }
            builder.append(part);
        }
        return builder.toString();
    }

    public static double[] overlap(double s1, double e1, double s2, double e2) {
        // thanks to https://stackoverflow.com/questions/9044084/efficient-date-range-overlap-calculation-in-python
        double latestStart = Math.max(s1, s2);
        double earliestEnd = Math.min(e1, e2);
        return new double[]{latestStart, earliestEnd};
    }

    public static <T extends Comparable<T>> int listCompare(List<T> l1, List<T> l2) {
        Iterator<T> it1 = l1.iterator();
        Iterator<T> it2 = l2.iterator();

        while (it1.hasNext() && it2.hasNext()) {
            T obj1 = it1.next();
            T obj2 = it2.next();
            int x = obj1.compareTo(obj2);
            if (x != 0) {
                return x;
            }
        }

        return Integer.compare(l1.size(), l2.size());
    }
}
