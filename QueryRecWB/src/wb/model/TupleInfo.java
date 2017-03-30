package wb.model;

import java.sql.ResultSet;

import largespace.business.DatabaseInteraction;

/**
 * This class stores all the needed tuple information for misc. operations in the {@link DatabaseInteraction}.
 * 
 * @author Otono Kakuei
 */
public class TupleInfo {
	private Long sequence;
	private Long tableId;
	private Object keyId;
	private boolean isKeyString;
	
	public TupleInfo(ResultSet resultSet, boolean isKeyString) {
		try {
			sequence = resultSet.getLong("SEQ");
			tableId = resultSet.getLong("TABLE_ID");
			if (isKeyString) {
				keyId = resultSet.getString("KEY_ID");
			} else {
				keyId = resultSet.getLong("KEY_ID");
			}
			this.isKeyString = isKeyString;
		} catch (Exception ex) {
			System.out.println("Exception in Getting data, seq = " + sequence);
		}
	}
	
	public boolean isKeyString() {
		return isKeyString;
	}
	
	public Long getSequence() {
		return sequence;
	}
	
	public Object getKeyId() {
		return keyId;
	}
	
	public Long getTableId() {
		return tableId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((keyId == null) ? 0 : keyId.hashCode());
		result = prime * result + ((tableId == null) ? 0 : tableId.hashCode());
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
		TupleInfo other = (TupleInfo) obj;
		if (keyId == null) {
			if (other.keyId != null)
				return false;
		} else if (!keyId.equals(other.keyId))
			return false;
		if (tableId == null) {
			if (other.tableId != null)
				return false;
		} else if (!tableId.equals(other.tableId))
			return false;
		return true;
	}
	

}
