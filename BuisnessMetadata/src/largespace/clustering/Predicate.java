package largespace.clustering;

import largespace.business.*;

import java.util.List;
import java.util.regex.Matcher;

public class Predicate implements Comparable<Predicate> {
	public String table;
	public String column;
	public final Operator op;
	public final String value;

	public Predicate(String s, String onlyTable, List<Table> ts) throws ParseException {
		Matcher m = Regex.operatorRegex.matcher(s);
		if (!m.find()) {
			throw new ParseException("Illegal predicate!", s);
		}

		String tableColumnString = s.substring(0, m.start());
		String[] tableColumn = Regex.dotRegex.split(tableColumnString);
		if (tableColumn.length == 2) {
			this.table = GlobalCaches.strings().deduplicate(tableColumn[0].trim().toLowerCase());
			this.column = GlobalCaches.strings().deduplicate(tableColumn[1].trim().toLowerCase());
		} else if (onlyTable != null) {
			this.table = GlobalCaches.strings().deduplicate(onlyTable.toLowerCase());
			this.column = GlobalCaches.strings().deduplicate(tableColumn[0].trim().toLowerCase());
		} else {
			// try to find out
			Boolean presentInOneTable = false;
			for (Table t : ts) {
				if (t.columns.containsKey(tableColumn[0].trim().toLowerCase())) {
					if (!presentInOneTable) {
						presentInOneTable = true;
						this.table = t.name;
						this.column = tableColumn[0].trim().toLowerCase();
					} else {
						this.table = t.name;
						this.column = tableColumn[0].trim().toLowerCase();
						break;
					}

				}
			}

			if (!presentInOneTable)
				throw new ParseException("Ambiguous column!", tableColumnString);
		}

		this.op = Operator.fromString(s.substring(m.start(), m.end()));

		this.value = s.substring(m.end()).trim();

		if (this.table.isEmpty() || this.column.isEmpty() || this.value.isEmpty()) {
			throw new ParseException("Empty predicate parts detected!", s);
		}
	}

	public Predicate(String s, String onlyTable) throws ParseException {
		Matcher m = Regex.operatorRegex.matcher(s);
		if (!m.find()) {
			throw new ParseException("Illegal predicate!", s);
		}

		String tableColumnString = s.substring(0, m.start());
		String[] tableColumn = Regex.dotRegex.split(tableColumnString);
		if (tableColumn.length == 2) {
			this.table = GlobalCaches.strings().deduplicate(tableColumn[0].trim().toLowerCase());
			this.column = GlobalCaches.strings().deduplicate(tableColumn[1].trim().toLowerCase());
		} else if (onlyTable != null) {
			this.table = GlobalCaches.strings().deduplicate(onlyTable.toLowerCase());
			this.column = GlobalCaches.strings().deduplicate(tableColumn[0].trim().toLowerCase());
		} else {
			throw new ParseException("Ambiguous column!", tableColumnString);
		}

		this.op = Operator.fromString(s.substring(m.start(), m.end()));

		this.value = s.substring(m.end()).trim();

		if (this.table.isEmpty() || this.column.isEmpty() || this.value.isEmpty()) {
			throw new ParseException("Empty predicate parts detected!", s);
		}
	}

	@Override
	public String toString() {
		return table + "." + column + " " + op.toString() + " " + value;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof Predicate)) {
			return false;
		} else {
			Predicate p2 = (Predicate) obj;
			return table.equals(p2.table) && column.equals(p2.column) && op == p2.op && value.equals(p2.value);
		}
	}

	@Override
	public int compareTo(Predicate other) {
		if (this == other) {
			return 0;
		}

		if (other == null) {
			return -1;
		}

		int result = 0;
		if (table != other.table) {
			result = table.compareTo(other.table);
			if (result != 0) {
				return result;
			}
		}

		if (column != other.column) {
			result = column.compareTo(other.column);
			if (result != 0) {
				return result;
			}
		}

		result = op.compareTo(other.op);
		if (result != 0) {
			return result;
		}

		return value.compareTo(other.value);
	}

	public double[] getAccessRange(double columnMin, double columnMax) {
		double v = Double.parseDouble(value);

		switch (op) {
		case LT:
		case LE: {
			double[] oRange = Utils.overlap(columnMin, columnMax, Double.NEGATIVE_INFINITY, v);
			if (oRange[0] <= oRange[1]) {
				return oRange;
			} else {
				// no overlap
				return new double[] { v, v };
			}
		}
		case GT:
		case GE: {
			double[] oRange = Utils.overlap(columnMin, columnMax, v, Double.POSITIVE_INFINITY);
			if (oRange[0] <= oRange[1]) {
				return oRange;
			} else {
				// no overlap
				return new double[] { v, v };
			}
		}
		case EQ:
			// no data
			return new double[] { v, v };
		case NE:
			// all data
			return new double[] { columnMin, columnMax };
		default:
			throw new Error("Unreachable!");
		}
	}

}
