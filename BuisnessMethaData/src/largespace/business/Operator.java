package largespace.business;

public enum Operator {
    LT, GT, LE, GE, EQ, NE;

    public static Operator fromString(String s) throws ParseException {
        s = s.trim();

        switch (s) {
            case "<":
                return LT;
            case ">":
                return GT;
            case "<=":
                return LE;
            case ">=":
                return GE;
            case "=":
                return EQ;
            case "<>":
                return NE;
            default:
                throw new ParseException("Illegal operator!", s);
        }
    }

    @Override
    public String toString() {
        switch (this) {
            case LT:
                return "<";
            case GT:
                return ">";
            case LE:
                return "<=";
            case GE:
                return ">=";
            case EQ:
                return "=";
            case NE:
                return "<>";
            default:
                throw new Error("Unreachable!");
        }
    }
}
