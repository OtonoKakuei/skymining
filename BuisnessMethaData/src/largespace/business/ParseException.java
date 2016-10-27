package largespace.business;

public class ParseException extends Exception {
    public final String data;

    public ParseException(String msg, String data) {
        super(msg);
        this.data = data;
    }

    @Override
    public String toString() {
        return "ParseException: " + super.getMessage() + " @ \"" + data + "\"";
    }
}
