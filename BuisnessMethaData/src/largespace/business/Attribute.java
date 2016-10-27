package largespace.business;

import java.sql.ResultSet;

public class Attribute {
	public Integer AttributeId;
	public String AttributeName;
	public Integer TableId;
	public Long[] AttributeMask = new Long[4];
	
	public Attribute (ResultSet rs)
	{
		try
		{
			//SELECT ATTRIBUTE_ID,			  TABLE_ID,			  ATTR_MASK_0,			  ATTR_MASK_1,			  ATTR_MASK_2,			  ATTR_MASK_3,			  ATTRIBUTE_NAME			FROM QRS_ATTRIBUTES ;
			AttributeId = rs.getInt("ATTRIBUTE_ID");
			TableId = rs.getInt("TABLE_ID");
			AttributeName =  rs.getString("ATTRIBUTE_NAME");
			AttributeMask = new Long[4];
			AttributeMask[0] = rs.getLong("ATTR_MASK_0");
			AttributeMask[1] = rs.getLong("ATTR_MASK_1");
			AttributeMask[2] = rs.getLong("ATTR_MASK_2");
			AttributeMask[3] = rs.getLong("ATTR_MASK_3");
		}
		catch (Exception ex)
		{
			System.out.println("Exception in Attribute(rs). ex = " + ex.getMessage());
		}
	}
}

