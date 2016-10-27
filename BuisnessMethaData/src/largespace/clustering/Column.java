package largespace.clustering;

import largespace.clustering.ValueState;
import largespace.business.ParseException;

import java.util.*;

public class Column {

	public enum GlobalColumnType {
		Identificator,
		DictionaryField,
		DistributedField,
		DistributedFieldWithEmissions,
		NonNumericidentifier
	}

	public Object Distribution;
	public GlobalColumnType GlobalColumnType;
	public String Name = "";
	public Integer AttributeId;
	public Integer TableId;
	public Integer AttributeType;

	// public Map<Object, ValueState> Values = new HashMap<Object,
	// ValueState>();
	public boolean SomethingWrong = false;

	public Column(String columnName) throws ParseException {
		Name = columnName;
	}

	public Column(String[] vals) throws ParseException {
		int clmnpropertySize = vals.length;
		if (clmnpropertySize >= 4) {
			Name = vals[0];
			switch (vals[1]) {
			case "DictionaryField": {
				GlobalColumnType = GlobalColumnType.DictionaryField;
				Distribution = new DictionaryField();
				int i = 3;
				while (i < clmnpropertySize) {
					String[] vals2 = vals[i].split(" ");
					String columnValue = vals2[0];
					Long valueCount = Long.parseLong(vals2[1]);
					Long accumulateCount = Long.parseLong(vals2[1]);
					((DictionaryField) Distribution).AddValue(columnValue,
							new ValueState(valueCount, accumulateCount, columnValue));
					i++;
				}
			}
				break;
			case "DistributedField": {
				GlobalColumnType = GlobalColumnType.DistributedField;
				Double minValue = Double.parseDouble(vals[2]);
				Double maxValue = Double.parseDouble(vals[3]);
				Distribution = new DistributedField(minValue, maxValue);

			}
				break;
			case "Identificator": {
				GlobalColumnType = GlobalColumnType.Identificator;
				Long minValue = Long.parseLong(vals[2]);
				Long maxValue = Long.parseLong(vals[3]);
				Long count = maxValue - minValue;
				Distribution = new Identificator(minValue, maxValue, count);
			}
				break;

			case "DistributedFieldWithEmissions": {
				GlobalColumnType = GlobalColumnType.DistributedFieldWithEmissions;
				Distribution = new DistributedFieldWithEmissions();
				Double minValue = Double.parseDouble(vals[2]);
				Double maxValue = Double.parseDouble(vals[3]);
				((DistributedFieldWithEmissions) Distribution).MinValue = minValue;
				((DistributedFieldWithEmissions) Distribution).MaxValue = maxValue;

				int i = 5;
				while (i < clmnpropertySize) {
					String[] vals2 = vals[i].split(" ");
					Object columnValue = vals2[0];
					Long valueCount = Long.parseLong(vals2[1]);
					Long accumulateCount = Long.parseLong(vals2[1]);
					((DistributedFieldWithEmissions) Distribution).AddValue(columnValue,
							new ValueState(valueCount, accumulateCount, columnValue));
					i++;
				}
			}
				break;
			}

		} else {
			switch (vals[1]) {
			case "NonNumericidentifier": {
				GlobalColumnType = GlobalColumnType.NonNumericidentifier;
				Name = vals[0];
			}
				break;
			}
		}
	}

	public Column(String columnName, Integer columnType, Integer attrId) {
		Name = columnName;
		AttributeType = columnType;
		AttributeId = attrId;
	}

}
