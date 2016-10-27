package largespace.business;

import largespace.clustering.Predicate;

import java.util.List;

public class GlobalCaches {
    private static StringCache stringsInstance;
    private static Cache<List<String>> fromPartsInstance;
    private static Cache<Predicate> predicatesInstance;
    private static Cache<List<Predicate>> clausesInstance;
    private static Cache<List<List<Predicate>>> wherePartsInstance;

    public static Cache<String> strings() {
        if (stringsInstance == null) {
            stringsInstance = new StringCache();
        }
        return stringsInstance;
    }

    public static Cache<List<String>> fromParts() {
        if (fromPartsInstance == null) {
            fromPartsInstance = new Cache<>();
        }
        return fromPartsInstance;
    }

    public static Cache<Predicate> predicates() {
        if (predicatesInstance == null) {
            predicatesInstance = new Cache<>();
        }
        return predicatesInstance;
    }

    public static Cache<List<Predicate>> clauses() {
        if (clausesInstance == null) {
            clausesInstance = new Cache<>();
        }
        return clausesInstance;
    }

    public static Cache<List<List<Predicate>>> whereParts() {
        if (wherePartsInstance == null) {
            wherePartsInstance = new Cache<>();
        }
        return wherePartsInstance;
    }

    public static void printStats() {
        System.out.println("Global caches:");
        System.out.println("  " + strings().size() + " unique strings");
        System.out.println("  " + fromParts().size() + " unique fromParts");
        System.out.println("  " + predicates().size() + " unique predicates");
        System.out.println("  " + clauses().size() + " unique clauses");
        System.out.println("  " + whereParts().size() + " unique whereParts");
    }
}
