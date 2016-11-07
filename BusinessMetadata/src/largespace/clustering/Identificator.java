package largespace.clustering;

import largespace.business.ParseException;

public class Identificator {
	public long minValue;
	public long maxValue;
	public long count;

	public Identificator(long minimumValue, long maximumValue, long count) throws ParseException {
		minValue = minimumValue;
		maxValue = maximumValue;
		this.count = count;
	}
}
