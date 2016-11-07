package largespace.business;

import java.sql.ResultSet;;

public class Feature {
	public String name = "";

	public Integer featureId;

	public Feature() {
	}

	public Feature(ResultSet rs, Boolean withkey) {
		if (withkey) {
			try {
				// from this recordset: FEATURE_ID, FEATURE
				featureId = rs.getInt("FEATURE_ID");
				name = rs.getString("FEATURE").toLowerCase();
			} catch (Exception ex) {
				System.out.println("Exception in Table(rs). ex = " + ex.getMessage());
			}
		}

	}

}
