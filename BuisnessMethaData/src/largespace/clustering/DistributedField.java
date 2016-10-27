package largespace.clustering;

import java.util.HashMap;
import java.util.Map;

public class DistributedField {
	public Double MinValue;
	public Double MaxValue;

	public Map<Object, ValueState> Values = new HashMap<Object, ValueState>();

	public DistributedField(Double minValue, Double maxValue) {
		MinValue = minValue;
		MaxValue = maxValue;

	}
}
