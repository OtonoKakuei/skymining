package largespace.clustering;

import java.util.HashMap;
import java.util.Map;

public class DistributedField {
	public Double minValue;
	public Double maxValue;

	public Map<Object, ValueState> values = new HashMap<Object, ValueState>();

	public DistributedField(Double minValue, Double maxValue) {
		this.minValue = minValue;
		this.maxValue = maxValue;

	}
}
