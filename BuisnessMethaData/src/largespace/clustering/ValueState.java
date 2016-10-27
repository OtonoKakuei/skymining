package largespace.clustering;

public class ValueState {
	public long valuesCount = 0;
	public long valuesLessOrEqualCount = 0;
	public Object value = null;

	public ValueState(long valueCount, long valuesLessOrEqualCount, Object value) {
		this.valuesCount = valueCount;
		this.valuesLessOrEqualCount = valuesLessOrEqualCount;
		this.value = value;
	}

	public ValueState() {
		valuesCount = 0;

	}
}