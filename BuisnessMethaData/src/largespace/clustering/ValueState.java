package largespace.clustering;

public class ValueState {
	public long ValuesCount = 0;
	public long ValuesLessOrEqualCount = 0;
	public Object Value = null;

	public ValueState(long valueCount, long valuesLessOrEqualCount, Object value) {
		ValuesCount = valueCount;
		ValuesLessOrEqualCount = valuesLessOrEqualCount;
		Value = value;
	}

	public ValueState() {
		ValuesCount = 0;

	}
}