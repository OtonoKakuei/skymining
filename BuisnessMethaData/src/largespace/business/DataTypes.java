package largespace.business;

public class DataTypes {

	public final static Integer typeLong = 1;
	public final static Integer typeFloat = 2;
	public final static Integer typeString = 3;
	public final static Integer typeDatetime = 4;

	public static Integer getDataTypeFromString(String type) {
		Integer res = -1;
		type = type.toLowerCase();
		switch (type) {
		case "bigint":
			res = typeLong;
			return res;
		case "numeric":
			res = typeLong;
			return res;
		case "bit":
			res = typeLong;
			return res;
		case "smallint":
			res = typeLong;
			return res;
		case "decimal":
			res = typeLong;
			return res;
		case "smallmoney":
			res = typeLong;
			return res;
		case "int":
			res = typeLong;
			return res;
		case "tinyint":
			res = typeLong;
			return res;
		case "money":
			res = typeLong;
			return res;

		case "float":
			res = typeFloat;
			return res;
		case "real":
			res = typeFloat;
			return res;

		case "date":
			res = typeDatetime;
			return res;
		case "datetimeoffset":
			res = typeDatetime;
			return res;
		case "datetime2":
			res = typeDatetime;
			return res;
		case "smalldatetime":
			res = typeDatetime;
			return res;
		case "datetime":
			res = typeDatetime;
			return res;
		case "time":
			res = typeDatetime;
			return res;

		case "char":
			res = typeString;
			return res;
		case "varchar":
			res = typeString;
			return res;
		case "text":
			res = typeString;
			return res;
		case "nchar":
			res = typeString;
			return res;
		case "nvarchar":
			res = typeString;
			return res;
		case "ntext":
			res = typeString;
			return res;

		}

		res = typeString;
		return res;
	}
}
