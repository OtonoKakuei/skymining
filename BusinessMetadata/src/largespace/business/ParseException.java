package largespace.business;

public class ParseException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = -9026554586775098768L;
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
