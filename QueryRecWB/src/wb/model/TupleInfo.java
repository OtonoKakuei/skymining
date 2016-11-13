package wb.model;

import java.sql.ResultSet;

public class TupleInfo {
	private Long sequence;
	private Long tableId;
	private Long keyId;
	
	public TupleInfo(ResultSet resultSet) {
		try {
			sequence = resultSet.getLong("SEQ");
			tableId = resultSet.getLong("TABLE_ID");
			keyId = resultSet.getLong("KEY_ID");
		} catch (Exception ex) {
			System.out.println("Exception in Getting data, seq = " + sequence);
		}
	}
	
	public Long getSequence() {
		return sequence;
	}
	
	public Long getKeyId() {
		return keyId;
	}
	
	public Long getTableId() {
		return tableId;
	}
	

}
