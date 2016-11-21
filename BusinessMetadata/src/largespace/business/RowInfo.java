package largespace.business;

import java.sql.ResultSet;

public class RowInfo {

	public String statement;
	public String fromStatement;
	public Long userSession;
	public Long seq;
	public Integer nrRows;

	public RowInfo(ResultSet rs) {
		try {
			seq = rs.getLong("seq");
			statement = rs.getString("STATEMENT");
			fromStatement = rs.getString("FROM_STATEMENT");
			userSession = rs.getLong("USERSESSION");
		} catch (Exception ex) {
			System.out.println("Exception in Getting data, seq = " + seq);
		}
	}

	public RowInfo(ResultSet rs, Boolean withNrRows) {
		if (withNrRows) {
			try {
				// from recordset seq, NRROWS, statement
				seq = rs.getLong("seq");
				statement = rs.getString("STATEMENT");
				fromStatement = rs.getString("FROM_STATEMENT");
				nrRows = rs.getInt("NRROWS");
			} catch (Exception ex) {
				System.out.println("Exception in Gettimg data, seq = " + seq);
			}
		}
	}
	
	@Override
	public String toString() {
		return "{ SEQ: " + seq + ", STATEMENT: " + statement + " }";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((nrRows == null) ? 0 : nrRows.hashCode());
		result = prime * result + ((seq == null) ? 0 : seq.hashCode());
		result = prime * result + ((statement == null) ? 0 : statement.hashCode());
		result = prime * result + ((userSession == null) ? 0 : userSession.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RowInfo other = (RowInfo) obj;
		if (nrRows == null) {
			if (other.nrRows != null)
				return false;
		} else if (!nrRows.equals(other.nrRows))
			return false;
		if (seq == null) {
			if (other.seq != null)
				return false;
		} else if (!seq.equals(other.seq))
			return false;
		if (statement == null) {
			if (other.statement != null)
				return false;
		} else if (!statement.equals(other.statement))
			return false;
		if (userSession == null) {
			if (other.userSession != null)
				return false;
		} else if (!userSession.equals(other.userSession))
			return false;
		return true;
	}
	
	
}
