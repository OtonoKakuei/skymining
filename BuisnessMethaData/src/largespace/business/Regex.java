package largespace.business;

import java.util.regex.Pattern;

public final class Regex {
    public static final Pattern operatorRegex = Pattern.compile("\\s(<|>|<=|>=|=|<>)\\s");
    public static final Pattern orRegex = Pattern.compile("\\sOR\\s");
    public static final Pattern andRegex = Pattern.compile("\\sAND\\s");
    public static final Pattern commaRegex = Pattern.compile(",\\s");
    public static final Pattern dotRegex = Pattern.compile("\\.");

    private Regex() {
    }
}
