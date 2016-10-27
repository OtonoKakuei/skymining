package largespace.clustering;

import java.util.HashMap;
import java.util.Map;

public class DistributedFieldWithEmissions {
	public Double minValue;
	public Double maxValue;

	public Map<Object, ValueState> values = new HashMap<Object, ValueState>();

	public void addValue(Object columnValue) {
		if (this.values.get(columnValue) != null) {
			// this.Values.put(columnValue, this.Values.get(columnValue) + 1);
		} else
			// because we should set this value even if it doesn't exist
			// somehow we need to find ValuesLessOrEqualCount value
			values.put(columnValue, new ValueState());

	}

	public void addValue(Object columnValue, ValueState valueState) {
		if (this.values.get(columnValue) != null) {
			// this.Values.put(columnValue, this.Values.get(columnValue) + 1);
		} else
			// because we should set this value even if it doesn't exist
			values.put(columnValue, valueState);
	}
}
