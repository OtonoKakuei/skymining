package largespace.business;

import java.sql.ResultSet;

public class RowInfo {

	public String statement;
	public Long userSession;
	public Long seq;
	public Integer nrRows;

	public RowInfo(ResultSet rs) {
		try {
			seq = rs.getLong("seq");
			statement = rs.getString("STATEMENT");
			userSession = rs.getLong("USERSESSION");
		} catch (Exception ex) {
			System.out.println("Exception in Gettimg data, seq = " + seq);
		}
	}

	public RowInfo(ResultSet rs, Boolean withNrRows) {
		if (withNrRows) {
			try {
				// from recordset seq, NRROWS, statement
				seq = rs.getLong("seq");
				statement = rs.getString("STATEMENT");
				nrRows = rs.getInt("NRROWS");
			} catch (Exception ex) {
				System.out.println("Exception in Gettimg data, seq = " + seq);
			}
		}
	}
}
