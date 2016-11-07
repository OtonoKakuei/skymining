package largespace.business;

import java.sql.ResultSet;

public class Attribute {
	public Integer attributeId;
	public String attributeName;
	public Integer tableId;
	public Long[] attributeMask = new Long[4];

	public Attribute(ResultSet rs) {
		try {
			// SELECT ATTRIBUTE_ID, TABLE_ID, ATTR_MASK_0, ATTR_MASK_1, ATTR_MASK_2, ATTR_MASK_3, ATTRIBUTE_NAME 
			// FROM QRS_ATTRIBUTES ;
			attributeId = rs.getInt("ATTRIBUTE_ID");
			tableId = rs.getInt("TABLE_ID");
			attributeName = rs.getString("ATTRIBUTE_NAME");
			attributeMask = new Long[4];
			attributeMask[0] = rs.getLong("ATTR_MASK_0");
			attributeMask[1] = rs.getLong("ATTR_MASK_1");
			attributeMask[2] = rs.getLong("ATTR_MASK_2");
			attributeMask[3] = rs.getLong("ATTR_MASK_3");
		} catch (Exception ex) {
			System.out.println("Exception in Attribute(rs). ex = " + ex.getMessage());
		}
	}
}
