package largespace.clustering;

import java.util.HashMap;
import java.util.Map;

import largespace.business.ParseException;

public class DistributedFieldWithEmissions {
	public Double MinValue;
	public Double MaxValue;

	public Map<Object, ValueState> Values = new HashMap<Object, ValueState>();

	public void AddValue(Object columnValue) {
		if (this.Values.get(columnValue) != null) {
			// this.Values.put(columnValue, this.Values.get(columnValue) + 1);
		} else
			// because we should set this value even if it doesn't exist
			// somehow we need to find ValuesLessOrEqualCount value
			Values.put(columnValue, new ValueState());

	}

	public void AddValue(Object columnValue, ValueState valueState) {
		if (this.Values.get(columnValue) != null) {
			// this.Values.put(columnValue, this.Values.get(columnValue) + 1);
		} else
			// because we should set this value even if it doesn't exist
			Values.put(columnValue, valueState);
	}
}
