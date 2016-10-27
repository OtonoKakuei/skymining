package largespace.clustering;

import largespace.business.ParseException;

public class Identificator {
	public long MinValue;
	public long MaxValue;
	public long Count;

	public Identificator(long minimumValue, long maximumValue, long count) throws ParseException {
		MinValue = minimumValue;
		MaxValue = maximumValue;
		Count = count;
	}
}
