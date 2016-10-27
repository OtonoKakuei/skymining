package largespace.business;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import aima.core.util.datastructure.Pair;
import largespace.clustering.Column;;
public class Feature {
	public String Name = "";
	
	public Integer FeatureId;
	
	public Feature()
	{}
	
	public Feature (ResultSet rs, Boolean withkey)
	{
		if (withkey)
		{
			try
			{
				// from this recordset: FEATURE_ID, FEATURE
				FeatureId = rs.getInt("FEATURE_ID");
				Name = rs.getString("FEATURE").toLowerCase();
			}
			catch (Exception ex)
			{
				System.out.println("Exception in Table(rs). ex = " + ex.getMessage());
			}
		}
		
	}

}
