package wb.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import largespace.business.DatabaseInteraction;

/**
 * This class stores all the needed session information for misc. operations in the {@link DatabaseInteraction}.
 * 
 * @author Otono Kakuei
 */
public class SessionInfo {
	private long sessionId;
	private List<Long> orderedSequences = new ArrayList<>();
	
	public SessionInfo(long sessionId, List<Long> orderedSequences) {
		this.sessionId = sessionId;
		this.orderedSequences.addAll(orderedSequences);
	}
	
	public List<Long> getOrderedSequences() {
		return orderedSequences;
	}
	
	public long getSessionId() {
		return sessionId;
	}
	
	@Override
	public String toString() {
		return "Session: " + sessionId + ", SEQs: " + Arrays.toString(orderedSequences.toArray());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((orderedSequences == null) ? 0 : orderedSequences.hashCode());
		result = prime * result + (int) (sessionId ^ (sessionId >>> 32));
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
		SessionInfo other = (SessionInfo) obj;
		if (orderedSequences == null) {
			if (other.orderedSequences != null)
				return false;
		} else if (!orderedSequences.equals(other.orderedSequences))
			return false;
		if (sessionId != other.sessionId)
			return false;
		return true;
	}
	
	
}
