package largespace.business;

import java.sql.ResultSet;

public class RowInfo {

	public String Statement;
	public Long UserSession;
	public Long Seq;
	public Integer NrRows;
	
	public RowInfo(ResultSet rs)
	{
		try
		{
		Seq = rs.getLong("seq");
		Statement = rs.getString("STATEMENT");
		UserSession = rs.getLong("USERSESSION");
		}
		catch (Exception ex)
		{
			System.out.println("Exception in Gettimg data, seq = "+ Seq);
		}
	}
	
	public RowInfo(ResultSet rs, Boolean withNrRows)
	{
		if (withNrRows)
		{
			try
			{
				// from recordset  seq, NRROWS, statement
			Seq = rs.getLong("seq");
			Statement = rs.getString("STATEMENT");
			NrRows = rs.getInt("NRROWS");
			}
			catch (Exception ex)
			{
				System.out.println("Exception in Gettimg data, seq = "+ Seq);
			}
		}
	}
}
