package largespace.clustering;

import java.util.Map;
import java.util.TreeMap;

public class DictionaryField {

	public Map<String, ValueState> values = new TreeMap<String, ValueState>();

	public void addValue(String columnValue) {
		if (this.values.get(columnValue) != null) {
			// this.Values.put(columnValue, this.Values.get(columnValue) + 1);
		} else
			// because we should set this value even if it doesn't exist
			// somehow we need to find ValuesLessOrEqualCount value
			values.put(columnValue, new ValueState());

	}

	public void addValue(String columnValue, ValueState valueState) {
		if (this.values.get(columnValue) != null) {
			// this.Values.put(columnValue, this.Values.get(columnValue) + 1);
		} else
			// because we should set this value even if it doesn't exist
			values.put(columnValue, valueState);
	}
}
