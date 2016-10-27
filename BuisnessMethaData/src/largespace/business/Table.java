package largespace.business;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import aima.core.util.datastructure.Pair;
import largespace.clustering.Column;;
public class Table {
	public String Name = "";
	public long Count = 0;
	public ArrayList<String> Links = new ArrayList<String>();
	
	public Integer TableId;
	public Long [] TableMask = new Long[4];
	public Map<String, Pair<Integer, Integer>> Columns = new HashMap<String, Pair<Integer, Integer>>();
	public Column KeyColumn;
	
	public Table()
	{}
	
	public Table (ResultSet rs, Boolean withkey)
	{
		if (withkey)
		{
			try
			{
				// from this recordset: TABLE_ID, TABLE_NAME,    COLUMN_NAME,    COLUMN_TYPE, ATTRIBUTE_ID, IS_KEY 
				TableId = rs.getInt("TABLE_ID");
				Name = rs.getString("TABLE_NAME").toLowerCase();
				Integer columnType = rs.getInt("COLUMN_TYPE");
				Integer attrId = rs.getInt("ATTRIBUTE_ID");
				String columnName = rs.getString("COLUMN_NAME").toLowerCase();
				KeyColumn = new Column(columnName, columnType,attrId );
				
			}
			catch (Exception ex)
			{
				System.out.println("Exception in Table(rs). ex = " + ex.getMessage());
			}
		}
		
	}
	
	public Table (ResultSet rs)
	{
		try
		{
			TableId = rs.getInt("TABLE_ID");
			Name = rs.getString("TABLE_NAME").toLowerCase();
			TableMask = new Long[4];
			TableMask[0] = rs.getLong("TABLE_MASK_0");
			TableMask[1] = rs.getLong("TABLE_MASK_1");
			TableMask[2] = rs.getLong("TABLE_MASK_2");
			TableMask[3] = rs.getLong("TABLE_MASK_3");
			Count = rs.getLong("COUNT");
		}
		catch (Exception ex)
		{
			System.out.println("Exception in Table(rs). ex = " + ex.getMessage());
		}
		
	}
	
	public Table (String[] vals)
	{
		int tblpropertySize = vals.length;
		if (tblpropertySize >= 4)
		{
			Name = vals[0];
			Count = Long.parseLong(vals[1]);
			int i = 3;
			while (i < tblpropertySize)
			{
				Links.add(vals[i]);
				i++;
			}
		}
			
	}
	
	public void setColumns(ResultSet rs)
	{
		try
		{
			//TABLE_NAME,    COLUMN_NAME,    COLUMN_TYPE
			while (rs.next()) {
				String column = rs.getString("TABLE_NAME") + "." + rs.getString("COLUMN_NAME");
				Integer dataType = rs.getInt("COLUMN_TYPE");
				Integer attrId = rs.getInt("ATTRIBUTE_ID");
				Pair<Integer, Integer> pair = new Pair(attrId, dataType);
				this.Columns.put(column, pair);
			}
		}
		catch (Exception ex)
		{
			System.out.println("Exception in setting column to the table. ex = " + ex.getMessage());
		}
	}
}
