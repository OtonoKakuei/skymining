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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((keyId == null) ? 0 : keyId.hashCode());
		result = prime * result + ((sequence == null) ? 0 : sequence.hashCode());
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
		if (sequence == null) {
			if (other.sequence != null)
				return false;
		} else if (!sequence.equals(other.sequence))
			return false;
		if (tableId == null) {
			if (other.tableId != null)
				return false;
		} else if (!tableId.equals(other.tableId))
			return false;
		return true;
	}
	

}
