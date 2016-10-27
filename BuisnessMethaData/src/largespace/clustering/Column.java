package largespace.clustering;

import largespace.business.ParseException;

public class Column {

	public enum GlobalColumnType {
		Identificator, DictionaryField, DistributedField, DistributedFieldWithEmissions, NonNumericidentifier
	}

	public Object distribution;
	public GlobalColumnType globalColumnType;
	public String name = "";
	public Integer attributeId;
	public Integer tableId;
	public Integer attributeType;

	// public Map<Object, ValueState> Values = new HashMap<Object,
	// ValueState>();
	public boolean somethingWrong = false;

	public Column(String columnName) throws ParseException {
		name = columnName;
	}

	public Column(String[] vals) throws ParseException {
		int clmnpropertySize = vals.length;
		if (clmnpropertySize >= 4) {
			name = vals[0];
			switch (vals[1]) {
			case "DictionaryField": {
				globalColumnType = GlobalColumnType.DictionaryField;
				distribution = new DictionaryField();
				int i = 3;
				while (i < clmnpropertySize) {
					String[] vals2 = vals[i].split(" ");
					String columnValue = vals2[0];
					Long valueCount = Long.parseLong(vals2[1]);
					Long accumulateCount = Long.parseLong(vals2[1]);
					((DictionaryField) distribution).addValue(columnValue,
							new ValueState(valueCount, accumulateCount, columnValue));
					i++;
				}
			}
				break;
			case "DistributedField": {
				globalColumnType = GlobalColumnType.DistributedField;
				Double minValue = Double.parseDouble(vals[2]);
				Double maxValue = Double.parseDouble(vals[3]);
				distribution = new DistributedField(minValue, maxValue);

			}
				break;
			case "Identificator": {
				globalColumnType = GlobalColumnType.Identificator;
				Long minValue = Long.parseLong(vals[2]);
				Long maxValue = Long.parseLong(vals[3]);
				Long count = maxValue - minValue;
				distribution = new Identificator(minValue, maxValue, count);
			}
				break;

			case "DistributedFieldWithEmissions": {
				globalColumnType = GlobalColumnType.DistributedFieldWithEmissions;
				distribution = new DistributedFieldWithEmissions();
				Double minValue = Double.parseDouble(vals[2]);
				Double maxValue = Double.parseDouble(vals[3]);
				((DistributedFieldWithEmissions) distribution).minValue = minValue;
				((DistributedFieldWithEmissions) distribution).maxValue = maxValue;

				int i = 5;
				while (i < clmnpropertySize) {
					String[] vals2 = vals[i].split(" ");
					Object columnValue = vals2[0];
					Long valueCount = Long.parseLong(vals2[1]);
					Long accumulateCount = Long.parseLong(vals2[1]);
					((DistributedFieldWithEmissions) distribution).addValue(columnValue,
							new ValueState(valueCount, accumulateCount, columnValue));
					i++;
				}
			}
				break;
			}

		} else {
			switch (vals[1]) {
			case "NonNumericidentifier": {
				globalColumnType = GlobalColumnType.NonNumericidentifier;
				name = vals[0];
			}
				break;
			}
		}
	}

	public Column(String columnName, Integer columnType, Integer attrId) {
		name = columnName;
		attributeType = columnType;
		attributeId = attrId;
	}

}
