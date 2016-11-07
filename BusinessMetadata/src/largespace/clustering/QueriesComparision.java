package largespace.clustering;

import java.io.BufferedWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import accessarea.AccessArea;
import accessarea.AccessAreaExtraction;
import aima.core.util.datastructure.Pair;
import largespace.business.Operator;
import largespace.business.Options;
import largespace.business.RowInfo;
import largespace.business.Table;
import largespace.clustering.Column.GlobalColumnType;

public final class QueriesComparision {

	public static Interval getTntervalForEq(Object val, Column column, Query q, Options opt, Table t) {
		Interval interval = new Interval();

		if (val.toString().contains(".")) {
			Boolean isNeedToCalculateInterval = IsNeedToCalculateInterval(val, opt, t);
			if (!isNeedToCalculateInterval)
				return null;
			// this is JOIN, we can't measure interval, only estimated intervals
			// row count
			interval.hasOnlyIntervalsEstimatedRowCount = true;
			Long estTCount = getEstimetedRowCountInParentTable(column, t);
			interval.estimatedRowCount = getEstimetedRowCountInJoin(val, column, q, opt, estTCount);
		}
		interval.minVal = val;
		interval.maxVal = val;
		interval.strictMinBorder = true;
		interval.strictMaxBorder = true;

		return interval;
	}

	static HashMap<Integer, List<Column>> getColumnsByType(ArrayList<String> columnsInQuery1, Options opt) {
		HashMap<Integer, List<Column>> columnsByType = new HashMap<Integer, List<Column>>();
		for (String column : columnsInQuery1) {
			Column c = opt.COLUMNS_DISTRIBUTION.get(column);
			switch (c.attributeType) {
			case 1:
				if (!columnsByType.containsKey(1)) {
					List<Column> clmns = new ArrayList<Column>();
					clmns.add(c);
					columnsByType.put(1, clmns);
				} else {
					List<Column> clmns = columnsByType.get(1);
					clmns.add(c);
					columnsByType.put(1, clmns);
				}
				break;
			case 2:
				if (!columnsByType.containsKey(2)) {
					List<Column> clmns = new ArrayList<Column>();
					clmns.add(c);
					columnsByType.put(2, clmns);
				} else {
					List<Column> clmns = columnsByType.get(2);
					clmns.add(c);
					columnsByType.put(2, clmns);
				}
				break;
			case 3:
				if (!columnsByType.containsKey(3)) {
					List<Column> clmns = new ArrayList<Column>();
					clmns.add(c);
					columnsByType.put(3, clmns);
				} else {
					List<Column> clmns = columnsByType.get(3);
					clmns.add(c);
					columnsByType.put(3, clmns);
				}
				break;

			}
		}
		return columnsByType;
	}

	public static Map<Long, Query> getSimilarQueriesForQureryAndColumn(Long statId, Long lastUs, Query q1, Table t,
			Options opt, Connection conn, Map<Long, Query> res, BufferedWriter writer) {
		try {
			// Turn use of the cursor on.
			// Map<String, Map<Integer, List<Predicate>>> columnsListpred1 =
			// getPredicateForEachColumn(q1, t, opt);
			Map<String, List<Interval>> mapColIntervals1 = getIntervalForQureryAndColumn(q1, t, opt);

			Set<String> columnsInQuery1_ = mapColIntervals1.keySet();

			ArrayList<String> columnsInQuery1 = new ArrayList<String>();
			columnsInQuery1.addAll(columnsInQuery1_);
			HashMap<Integer, List<Column>> columnsByType = getColumnsByType(columnsInQuery1, opt);

			for (String column : columnsInQuery1) {
				List<Interval> intervals = mapColIntervals1.get(column);
				Column c = opt.COLUMNS_DISTRIBUTION.get(column);
				Long tmp = new Long(0);
				if (intervals.size() != 0) {
					String columnName = c.name.toLowerCase();
					Pair<Integer, Integer> attrIdAnddataType = t.columns.get(columnName);
					res = getQueriesWithSimilarIntervals(statId, lastUs, t, c, intervals, conn, attrIdAnddataType, opt,
							res, writer, columnsByType);

					if (res.size() == 0)
						res = getQueriesWithSimilarAttributes(statId, lastUs, t, c, intervals, conn, attrIdAnddataType,
								opt, res, writer, columnsByType);
				}

			}
		} catch (Exception ex) {
			System.out.println("ex = " + ex);
			return null;
		}

		return res;
	}

	// columnByType consist of List of columns grouped by type (numeric, float
	// or string)
	private static Map<Long, Query> getQueriesWithSimilarIntervalsNumber(Long statId, Long lastUs, Table t, Column c,
			List<Interval> intervalsInQuery1, Connection conn, Pair<Integer, Integer> attrIdAnddataType,
			Map<Long, Query> res, String tableName, Options opt, BufferedWriter writer,
			HashMap<Integer, List<Column>> columnByType) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			Integer attrIdsSize = 0;
			ResultSet rs = null;
			AccessAreaExtraction extraction = new AccessAreaExtraction();
			AccessArea accessArea;

			for (Interval intl : intervalsInQuery1) {
				String theSetOfAttributes = "";
				String minVal = "";
				String maxVal = "";
				if (attrIdAnddataType.getSecond() == 1) {
					attrIdsSize = columnByType.get(1).size();
					for (Column c1 : columnByType.get(1)) {
						theSetOfAttributes = theSetOfAttributes + c1.attributeId + ",";
					}
					theSetOfAttributes = theSetOfAttributes.substring(0, theSetOfAttributes.length() - 1);
					Long val = new Long(0);
					if (intl.minVal.toString().contains("x"))
						val = Long.parseLong(intl.minVal.toString().replace("0x", "").replace("x", ""), 16);
					else {
						try {
							val = Long.parseLong(intl.minVal.toString());
						} catch (Exception ex) {
							///// System.out.println("column.Name = " +
							///// column.Name + "; ex = " + ex);
							return null;
						}
					}
					minVal = val.toString();
					if (intl.maxVal.toString().contains("x"))
						val = Long.parseLong(intl.maxVal.toString().replace("0x", "").replace("x", ""), 16);
					else {
						try {
							val = Long.parseLong(intl.maxVal.toString());
						} catch (Exception ex) {
							///// System.out.println("column.Name = " +
							///// column.Name + "; ex = " + ex);
							return null;
						}
					}
					maxVal = val.toString();

				}
				if (attrIdAnddataType.getSecond() == 2) {
					attrIdsSize = columnByType.get(2).size();
					for (Column c1 : columnByType.get(2)) {
						theSetOfAttributes = theSetOfAttributes + c1.attributeId + ",";
					}
					theSetOfAttributes = theSetOfAttributes.substring(0, theSetOfAttributes.length() - 1);

					minVal = ((Double) Double.parseDouble(intl.minVal.toString())).toString();
					maxVal = ((Double) Double.parseDouble(intl.maxVal.toString())).toString();
				}

				// we need to return queries with the same set of filtering
				// attributes

				// String query = "select s.seq, s.STATEMENT, sa.min_val,
				// sa.max_val, us.USERSESSION from " +
				// "QRS_STATEMENTS_PP s inner join QRS_USER_SESSIONS_PP us on
				// us.seq = s.seq and us.thetime = s.thetime inner join (select
				// sa.stat_id as stat_id, sa.min_val, sa.max_val from " +
				// tableName + " sa where sa.ATTR_ID = " + c.AttributeId + " and
				// (MIN_VAL >= " + minVal + ") and (MAX_VAL <= " + maxVal + ") "
				// +
				// ") sa on sa.stat_id = s.seq where s.seq <> " + statId +
				// " and us.usersession <> " + lastUs + " order by MIN_VAL,
				// max_val";

				String query = "select min(s.seq) as SEQ, s.STATEMENT, sa.min_val, sa.max_val, us.USERSESSION  "
						+ "from QRS_STATEMENTS_PP s inner join QRS_USER_SESSIONS_PP us on us.seq = s.seq and us.thetime = s.thetime "
						+ "inner join (select sa.stat_id as stat_id, sa.min_val, sa.max_val from " + tableName + " sa "
						+ "where sa.ATTR_ID = " + c.attributeId + " and (MIN_VAL >= " + minVal + ") and (MAX_VAL <= "
						+ maxVal + ")	) sa on sa.stat_id = s.seq "
						+ "inner join (select sa.stat_id , count(distinct sa.ATTR_ID) as cattr from " + tableName
						+ " sa " + "where sa.ATTR_ID in (" + theSetOfAttributes + ") " + "group by sa.stat_id "
						+ "having count(distinct sa.ATTR_ID) = " + attrIdsSize + ") q on q.stat_id = s.seq "
						+ "inner join (select sa.stat_id , count(distinct sa.ATTR_ID) as cattr from " + tableName
						+ " sa " + "group by sa.stat_id) q2 on q2.stat_id = q.stat_id and q2.cattr = q.cattr "
						+ "where s.seq <> " + statId + " and us.usersession <> " + lastUs
						+ " group by s.STATEMENT, sa.min_val, sa.max_val, us.USERSESSION "
						+ "order by MIN_VAL, max_val";
				writer.write(query + "\n");
				rs = st.executeQuery(query);

				Integer n = 100;
				List<RowInfo> rowInfos = getResiultsFromResultSet(rs, n);

				rs.close();

				// Write the interval
				// read the result (not all the results, only top n)

				for (RowInfo ri : rowInfos) {
					Long seq = ri.seq;
					if (!res.containsKey(seq)) {
						String statement = ri.statement;
						Long userSession = ri.userSession;
						try {

							accessArea = extraction.extractAccessArea(statement);

							String from = "";
							String where = "";
							from = accessArea.getFrom().toString();
							from = from.substring(1, from.length() - 1);
							where = accessArea.getWhere().toString();
							Query q = new Query(0, from, where, seq, opt, userSession);
							q.statement = statement;
							res.put(seq, q);

						} catch (Exception ex) {
							accessArea = null;
							System.out.println("Could not exctract access area :(");
						}
					}
				}

			}
		} catch (Exception ex) {
			System.out.println(ex);
		}
		return res;
	}

	private static Map<Long, Query> getQueriesWithSimilarAttributesNumber(Long statId, Long lastUs, Table t, Column c,
			List<Interval> intervalsInQuery1, Connection conn, Pair<Integer, Integer> attrIdAnddataType,
			Map<Long, Query> res, String tableName, Options opt, BufferedWriter writer,
			HashMap<Integer, List<Column>> columnsByType) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			ResultSet rs = null;
			AccessAreaExtraction extraction = new AccessAreaExtraction();
			AccessArea accessArea;

			for (Interval intl : intervalsInQuery1) {
				Integer attrIdsSize = 0;
				String theSetOfAttributes = "";
				if (attrIdAnddataType.getSecond() == 2) {
					attrIdsSize = columnsByType.get(2).size();
					for (Column c1 : columnsByType.get(2)) {
						theSetOfAttributes = theSetOfAttributes + c1.attributeId + ",";
					}
					theSetOfAttributes = theSetOfAttributes.substring(0, theSetOfAttributes.length() - 1);
				}
				if (attrIdAnddataType.getSecond() == 1) {
					attrIdsSize = columnsByType.get(1).size();
					for (Column c1 : columnsByType.get(1)) {
						theSetOfAttributes = theSetOfAttributes + c1.attributeId + ",";
					}
					theSetOfAttributes = theSetOfAttributes.substring(0, theSetOfAttributes.length() - 1);
				}
				List<RowInfo> rowInfos = new ArrayList<RowInfo>();
				String minVal = "";
				String maxVal = "";
				String query = "";
				{

					query = "select min(s.seq) as SEQ, s.STATEMENT, sa.min_val, sa.max_val, us.USERSESSION  "
							+ "from QRS_STATEMENTS_PP s inner join QRS_USER_SESSIONS_PP us on us.seq = s.seq and us.thetime = s.thetime "
							+ "inner join (select sa.stat_id as stat_id, sa.min_val, sa.max_val from " + tableName
							+ " sa " + "where sa.ATTR_ID = " + c.attributeId + " ) sa on sa.stat_id = s.seq "
							+ "inner join (select sa.stat_id , count(distinct sa.ATTR_ID) as cattr from " + tableName
							+ " sa " + "where sa.ATTR_ID in (" + theSetOfAttributes + ") " + "group by sa.stat_id "
							+ "having count(distinct sa.ATTR_ID) = " + attrIdsSize + ") q on q.stat_id = s.seq "
							+ "inner join (select sa.stat_id , count(distinct sa.ATTR_ID) as cattr from " + tableName
							+ " sa " + "group by sa.stat_id) q2 on q2.stat_id = q.stat_id and q2.cattr = q.cattr "
							+ "where s.seq <> " + statId + " and us.usersession <> " + lastUs
							+ " group by s.STATEMENT, sa.min_val, sa.max_val, us.USERSESSION "
							+ "order by MIN_VAL, max_val";
					// query = "select s.seq, s.STATEMENT, sa.min_val,
					// sa.max_val, us.USERSESSION from " +
					// "QRS_STATEMENTS_PP s inner join QRS_USER_SESSIONS_PP us
					// on us.seq = s.seq and us.thetime = s.thetime inner join
					// (select sa.stat_id as stat_id, sa.min_val, sa.max_val
					// from " +
					// tableName + " sa where sa.ATTR_ID = " + c.AttributeId + "
					// " +
					// ") sa on sa.stat_id = s.seq where s.seq <> " + statId +
					// " and us.usersession <> " + lastUs + " order by MIN_VAL,
					// max_val";

					writer.write(query + "\n");
					rs = st.executeQuery(query);

					Integer n = 10;
					rowInfos = getResiultsFromResultSet(rs, n);
				}
				rs.close();

				// Write the interval
				// read the result (not all the results, only top n)

				for (RowInfo ri : rowInfos) {
					Long seq = ri.seq;
					if (!res.containsKey(seq)) {
						String statement = ri.statement;
						Long userSession = ri.userSession;
						try {

							accessArea = extraction.extractAccessArea(statement);

							String from = "";
							String where = "";
							from = accessArea.getFrom().toString();
							from = from.substring(1, from.length() - 1);
							where = accessArea.getWhere().toString();
							Query q = new Query(0, from, where, seq, opt, userSession);
							q.statement = statement;
							q.hasNotOverlapped = true;
							res.put(seq, q);

						} catch (Exception ex) {
							accessArea = null;
							System.out.println("Could not exctract access area :(");
						}
					}
				}

			}
		} catch (Exception ex) {
			System.out.println(ex);
		}
		return res;
	}

	private static Map<Long, Query> getQueriesWithSimilarAttributesString(Long statId, Long lastUs, Table t, Column c,
			List<Interval> intervalsInQuery1, Connection conn, Pair<Integer, Integer> attrIdAnddataType,
			Map<Long, Query> res, String tableName, Options opt, BufferedWriter writer,
			HashMap<Integer, List<Column>> columnsByType) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			ResultSet rs = null;
			AccessAreaExtraction extraction = new AccessAreaExtraction();
			AccessArea accessArea;

			for (Interval intl : intervalsInQuery1) {
				Integer attrIdsSize = columnsByType.get(3).size();
				String theSetOfAttributes = "";
				for (Column c1 : columnsByType.get(3)) {
					theSetOfAttributes = theSetOfAttributes + c1.attributeId + ",";
				}
				theSetOfAttributes = theSetOfAttributes.substring(0, theSetOfAttributes.length() - 1);

				String query = "select min(s.seq) as SEQ, s.STATEMENT, sa.min_val, sa.max_val, us.USERSESSION  "
						+ "from QRS_STATEMENTS_PP s inner join QRS_USER_SESSIONS_PP us on us.seq = s.seq and us.thetime = s.thetime "
						+ "inner join (select sa.stat_id as stat_id, sa.min_val, sa.max_val from " + tableName + " sa "
						+ "where sa.ATTR_ID = " + c.attributeId + " ) sa on sa.stat_id = s.seq "
						+ "inner join (select sa.stat_id , count(distinct sa.ATTR_ID) as cattr from " + tableName
						+ " sa " + "where sa.ATTR_ID in (" + theSetOfAttributes + ") " + "group by sa.stat_id "
						+ "having count(distinct sa.ATTR_ID) = " + attrIdsSize + ") q on q.stat_id = s.seq "
						+ "inner join (select sa.stat_id , count(distinct sa.ATTR_ID) as cattr from " + tableName
						+ " sa " + "group by sa.stat_id) q2 on q2.stat_id = q.stat_id and q2.cattr = q.cattr "
						+ "where s.seq <> " + statId + " and us.usersession <> " + lastUs
						+ " group by s.STATEMENT, sa.min_val, sa.max_val, us.USERSESSION "
						+ "order by MIN_VAL, max_val";

				// String query = "select s.seq, s.STATEMENT, sa.min_val,
				// sa.max_val, us.USERSESSION from " +
				// "QRS_STATEMENTS_PP s inner join QRS_USER_SESSIONS_PP us on
				// us.seq = s.seq and us.thetime = s.thetime inner join (select
				// min(sa.stat_id) as stat_id, sa.min_val, sa.max_val from " +
				// tableName + " sa where sa.ATTR_ID = " + c.AttributeId + "
				// group by sa.min_val, sa.max_val" +
				// ") sa on sa.stat_id = s.seq where s.seq <> " + statId +
				// " and us.usersession <> " + lastUs + " order by MIN_VAL,
				// max_val";

				writer.write(query + "\n");
				rs = st.executeQuery(query);

				Integer n = 10;
				List<RowInfo> rowInfos = getResiultsFromResultSet(rs, n);

				rs.close();

				// Write the interval
				// read the result (not all the results, only top n)

				for (RowInfo ri : rowInfos) {
					Long seq = ri.seq;
					if (!res.containsKey(seq)) {
						String statement = ri.statement;
						Long userSession = ri.userSession;
						try {

							accessArea = extraction.extractAccessArea(statement);

							String from = "";
							String where = "";
							from = accessArea.getFrom().toString();
							from = from.substring(1, from.length() - 1);
							where = accessArea.getWhere().toString();
							Query q = new Query(0, from, where, seq, opt, userSession);
							q.statement = statement;
							q.hasNotOverlapped = true;
							res.put(seq, q);

						} catch (Exception ex) {
							accessArea = null;
							System.out.println("Could not exctract access area :(");
						}
					}
				}

			}
		} catch (Exception ex) {
			System.out.println(ex);
		}
		return res;
	}

	private static Map<Long, Query> getQueriesWithSimilarIntervalsString(Long statId, Long lastUs, Table t, Column c,
			List<Interval> intervalsInQuery1, Connection conn, Pair<Integer, Integer> attrIdAnddataType,
			Map<Long, Query> res, String tableName, Options opt, BufferedWriter writer,
			HashMap<Integer, List<Column>> columnsByType) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			ResultSet rs = null;
			AccessAreaExtraction extraction = new AccessAreaExtraction();
			AccessArea accessArea;

			for (Interval intl : intervalsInQuery1) {
				Integer attrIdsSize = columnsByType.get(3).size();
				String theSetOfAttributes = "";
				for (Column c1 : columnsByType.get(3)) {
					theSetOfAttributes = theSetOfAttributes + c1.attributeId + ",";
				}
				theSetOfAttributes = theSetOfAttributes.substring(0, theSetOfAttributes.length() - 1);

				String minVal = "";
				String maxVal = "";
				// usually for string min_val and max_val are the same
				if (attrIdAnddataType.getSecond() == 3) {
					minVal = intl.minVal.toString();
					maxVal = intl.maxVal.toString();

				}

				String query = "select min(s.seq) as SEQ, s.STATEMENT, sa.min_val, sa.max_val, us.USERSESSION  "
						+ "from QRS_STATEMENTS_PP s inner join QRS_USER_SESSIONS_PP us on us.seq = s.seq and us.thetime = s.thetime "
						+ "inner join (select sa.stat_id as stat_id, sa.min_val, sa.max_val from " + tableName + " sa "
						+ "where sa.ATTR_ID = " + c.attributeId + " and (MIN_VAL = " + minVal + ") and (MAX_VAL = "
						+ maxVal + ")	) sa on sa.stat_id = s.seq "
						+ "inner join (select sa.stat_id , count(distinct sa.ATTR_ID) as cattr from " + tableName
						+ " sa " + "where sa.ATTR_ID in (" + theSetOfAttributes + ") " + "group by sa.stat_id "
						+ "having count(distinct sa.ATTR_ID) = " + attrIdsSize + ") q on q.stat_id = s.seq "
						+ "inner join (select sa.stat_id , count(distinct sa.ATTR_ID) as cattr from " + tableName
						+ " sa " + "group by sa.stat_id) q2 on q2.stat_id = q.stat_id and q2.cattr = q.cattr "
						+ "where s.seq <> " + statId + " and us.usersession <> " + lastUs
						+ " group by s.STATEMENT, sa.min_val, sa.max_val, us.USERSESSION "
						+ "order by MIN_VAL, max_val";

				// String query = "select s.seq, s.STATEMENT, sa.min_val,
				// sa.max_val, us.USERSESSION from " +
				// "QRS_STATEMENTS_PP s inner join QRS_USER_SESSIONS_PP us on
				// us.seq = s.seq and us.thetime = s.thetime inner join (select
				// sa.stat_id as stat_id, sa.min_val, sa.max_val from " +
				// tableName + " sa where sa.ATTR_ID = " + c.AttributeId + " and
				// (MIN_VAL = " + minVal + ") and (MAX_VAL = " + maxVal + ") " +
				// ") sa on sa.stat_id = s.seq where s.seq <> " + statId +
				// " and us.usersession <> " + lastUs + " order by MIN_VAL,
				// max_val";

				writer.write(query + "\n");
				rs = st.executeQuery(query);

				Integer n = 10;
				List<RowInfo> rowInfos = getResiultsFromResultSet(rs, n);

				rs.close();

				// Write the interval
				// read the result (not all the results, only top n)

				for (RowInfo ri : rowInfos) {
					Long seq = ri.seq;
					if (!res.containsKey(seq)) {
						String statement = ri.statement;
						Long userSession = ri.userSession;
						try {

							accessArea = extraction.extractAccessArea(statement);

							String from = "";
							String where = "";
							from = accessArea.getFrom().toString();
							from = from.substring(1, from.length() - 1);
							where = accessArea.getWhere().toString();
							Query q = new Query(0, from, where, seq, opt, userSession);
							q.statement = statement;
							res.put(seq, q);

						} catch (Exception ex) {
							accessArea = null;
							System.out.println("Could not exctract access area :(");
						}
					}
				}

			}
		} catch (Exception ex) {
			System.out.println(ex);
		}
		return res;
	}

	public static List<RowInfo> getResiultsFromResultSet(ResultSet rs, Integer n) {
		List<RowInfo> rowInfos = new ArrayList<RowInfo>();
		Integer i = 0;

		try {
			while (rs.next() && (i < n)) {
				i++;
				RowInfo ri = new RowInfo(rs);
				rowInfos.add(ri);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowInfos;

	}

	public static Map<Long, Query> getQueriesWithSimilarIntervals(Long statId, Long lastUs, Table t, Column c,
			List<Interval> intervalsInQuery1, Connection conn, Pair<Integer, Integer> attrIdAnddataType, Options opt,
			Map<Long, Query> res, BufferedWriter writer, HashMap<Integer, List<Column>> columnByType) {
		// public final static Integer typeLong = 1;
		// public final static Integer typeFloat = 2;
		// public final static Integer typeString = 3;
		// public final static Integer typeDatetime = 4;

		switch (attrIdAnddataType.getSecond()) {
		case 1:
			res = getQueriesWithSimilarIntervalsNumber(statId, lastUs, t, c, intervalsInQuery1, conn, attrIdAnddataType,
					res, "QRS_STAT_ATTR_NUMBER", opt, writer, columnByType);
			break;
		case 2:
			res = getQueriesWithSimilarIntervalsNumber(statId, lastUs, t, c, intervalsInQuery1, conn, attrIdAnddataType,
					res, "QRS_STAT_ATTR_FLOAT", opt, writer, columnByType);
			break;
		case 3:
			res = getQueriesWithSimilarIntervalsString(statId, lastUs, t, c, intervalsInQuery1, conn, attrIdAnddataType,
					res, "QRS_STAT_ATTR_STRING", opt, writer, columnByType);
			break;
		case 4:
			writeDateInterval(statId, t, c, intervalsInQuery1, conn, attrIdAnddataType);
			break;
		}

		return res;
	}

	public static Map<Long, Query> getQueriesWithSimilarAttributes(Long statId, Long lastUs, Table t, Column c,
			List<Interval> intervalsInQuery1, Connection conn, Pair<Integer, Integer> attrIdAnddataType, Options opt,
			Map<Long, Query> res, BufferedWriter writer, HashMap<Integer, List<Column>> columnsByType) {
		// public final static Integer typeLong = 1;
		// public final static Integer typeFloat = 2;
		// public final static Integer typeString = 3;
		// public final static Integer typeDatetime = 4;

		switch (attrIdAnddataType.getSecond()) {
		case 1:
			res = getQueriesWithSimilarAttributesNumber(statId, lastUs, t, c, intervalsInQuery1, conn,
					attrIdAnddataType, res, "QRS_STAT_ATTR_NUMBER", opt, writer, columnsByType);
			break;
		case 2:
			res = getQueriesWithSimilarAttributesNumber(statId, lastUs, t, c, intervalsInQuery1, conn,
					attrIdAnddataType, res, "QRS_STAT_ATTR_FLOAT", opt, writer, columnsByType);
			break;
		case 3:
			res = getQueriesWithSimilarAttributesString(statId, lastUs, t, c, intervalsInQuery1, conn,
					attrIdAnddataType, res, "QRS_STAT_ATTR_STRING", opt, writer, columnsByType);
			break;
		case 4:
			writeDateInterval(statId, t, c, intervalsInQuery1, conn, attrIdAnddataType);
			break;
		}

		return res;
	}

	private static Map<Long, String> getNextQuery(Long userSession, Long seq, Connection conn, BufferedWriter writer) {
		HashMap<Long, String> res = new HashMap<Long, String>();
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			AccessAreaExtraction extraction = new AccessAreaExtraction();
			AccessArea accessArea;

			// select qs.seq, qs.statement, case when (qs.seq > 524298) then
			// qs.seq - 524298 else 524298 - qs.seq end as dist
			// from QRS_STATEMENTS_PP qs inner join QRS_USER_SESSIONS_PP us on
			// us.seq = qs.seq and us.thetime = qs.thetime
			// where us.USERSESSION = 247 and us.seq in (select seq from
			// QRS_USER_SESSIONS_PP where USERSESSION = 247 and seq <> 524298)
			// order by case when (qs.seq > 524298) then qs.seq - 524298 else
			// 524298 - qs.seq end;

			String query = "select qs.seq, qs.statement,case when  (qs.seq > " + seq + ") then qs.seq - " + seq
					+ " else " + seq + "- qs.seq  end as dist			from QRS_STATEMENTS_PP qs"
					+ "	inner join QRS_USER_SESSIONS_PP us on us.seq = qs.seq and us.thetime = qs.thetime "
					+ "where us.USERSESSION = " + userSession
					+ " and us.seq in (select seq from QRS_USER_SESSIONS_PP where USERSESSION = " + userSession
					+ " and seq <> " + seq + ") order by case when  (qs.seq > " + seq + ") then qs.seq - " + seq
					+ " else " + seq + "- qs.seq end";

			// Writer.write(query + "\n");
			rs = st.executeQuery(query);
			// Write the interval
			// read the result (not all the results, only top n)
			Integer n = 8;
			Integer i = 0;
			while (rs.next()) {
				if (i < n) {
					Long seq1 = rs.getLong("seq");
					String statement = rs.getString("statement");
					res.put(seq1, statement);
					i++;
				} else
					break;

			}

			rs.close();

		} catch (Exception ex) {
			System.out.println(ex);
		}
		return res;
	}

	public static Map<Long, Query> getNextQueriesForUserSessions(Map<Long, Pair<Query, Double>> theNearestQueries,
			Connection conn, BufferedWriter writer, Options opt) {
		try {
			Map<Long, Query> res = new TreeMap<Long, Query>();
			for (Long sq : theNearestQueries.keySet()) {
				Pair<Query, Double> theQueryWithDist = null;

				try {
					theQueryWithDist = theNearestQueries.get(sq);
				} catch (Exception ex) {
					System.out.println(ex);
				}
				Map<Long, String> nextQ = null;
				try {
					nextQ = getNextQuery(theQueryWithDist.getFirst().userSession, sq, conn, writer);
				} catch (Exception ex) {
					System.out.println(ex);
				}
				for (Long v : nextQ.keySet()) {

					if (theQueryWithDist.getFirst().hasNotOverlapped == true) {
						// it doesn't matter what filtering condition the query
						// has
						String statement = nextQ.get(v);
						Query q = CreateQuery(statement, v, opt, theQueryWithDist.getFirst().userSession);
						if (q != null) {
							Boolean doNotAdd = false;
							for (Query cq : res.values()) {
								if (cq.fromPart.equals(q.fromPart)) {
									doNotAdd = true;
								}
							}
							if (!doNotAdd)
								res.put(v, q);
						}
					} else {
						try {
							String statement = nextQ.get(v);
							Query q = CreateQuery(statement, v, opt, theQueryWithDist.getFirst().userSession);
							if (q != null) {
								Boolean doNotAdd = false;
								for (Query cq : res.values()) {
									if (cq.statement.equals(q.statement)) {
										doNotAdd = true;
									}
								}
								if (!doNotAdd) {
									try {
										res.put(v, q);
									} catch (Exception ex2) {
										System.out.println(ex2);
									}
								}
							}
						} catch (Exception ex) {
							System.out.println(ex);
						}
						// res.put(v, q);
					}
				}
				// res.put(nextQ.getFirst(), nextQ.getSecond());
			}
			return res;
		} catch (Exception ex) {
			System.out.println(ex);
			return new TreeMap<Long, Query>();
		}
	}

	public static Query CreateQuery(String statement, Long seq, Options opt, Long userSession) {
		try {
			AccessAreaExtraction extraction = new AccessAreaExtraction();
			Query q = null;
			AccessArea accessArea;
			accessArea = extraction.extractAccessArea(statement);
			String from = "";
			String where = "";
			from = accessArea.getFrom().toString();
			from = from.substring(1, from.length() - 1);
			where = accessArea.getWhere().toString();

			if (where.equals("NULL") | where.equals("TRUE")) {
				where = "";
			}

			if (!from.equals("") || !where.equals("")) {
				try {
					q = new Query(0, from, where, seq, opt, userSession);
					q.statement = statement;
				} catch (Exception ex) {

				}
			}
			return q;
		} catch (Exception ex) {
			return null;
		}
	}

	public static Long getEstimetedRowCountInParentTable(Column column, Table t) {
		Long res = t.count;
		if (column.globalColumnType == GlobalColumnType.DistributedFieldWithEmissions) {
			for (ValueState vs : ((DistributedFieldWithEmissions) column.distribution).values.values()) {
				res = vs.valuesCount;
			}
		}
		return res;
	}

	public static Boolean IsNeedToCalculateInterval(Object val, Options opt, Table t) {
		Boolean res = false;
		String[] vals = val.toString().split("\\.");
		String tableName = vals[0];
		String columnName = vals[1];
		Table t1 = opt.TABLESWITHCOUNT.get(tableName);
		if (t1 == null)
			return false;
		if (t.count > t1.count)
			res = true;
		return res;
	}

	public static long getEstimetedRowCountInJoin(Object val, Column column, Query q, Options opt, Long estTCount) {
		Long res = new Long(0);
		String[] vals = val.toString().split("\\.");
		String tableName = vals[0];
		String columnName = vals[1];
		Table t = opt.TABLESWITHCOUNT.get(tableName);
		res = t.count;
		Column c = opt.COLUMNS_DISTRIBUTION.get(tableName + "." + columnName);
		if (c == null) {
			// System.out.println("column = " + columnName + "is null");
			return 0;
		} else {
			Map<String, List<Interval>> mapColIntervals1 = getIntervalForQureryAndColumn(q, t, opt);
			if (mapColIntervals1 == null)
				return -1;
			Set<String> columnsInQuery1_ = mapColIntervals1.keySet();

			ArrayList<String> columnsInQuery1 = new ArrayList<String>();
			columnsInQuery1.addAll(columnsInQuery1_);

			Map<String, List<Interval>> mapColIntervalsAll = new TreeMap<String, List<Interval>>();
			for (String columnstr : columnsInQuery1) {
				List<Interval> intervals = mapColIntervals1.get(columnstr);
				Column c1 = opt.COLUMNS_DISTRIBUTION.get(columnstr);
				Long tmp = getEstimatedRowsCount(intervals, c1, t);
				if (tmp < res)
					res = tmp;
			}

		}
		Double k = ((Long) t.count).doubleValue() / estTCount;
		return Math.round(res * k);
	}

	public static Interval getTntervalForLe(Object val, Column column) {
		Interval interval = new Interval();
		GlobalColumnType columnType = column.globalColumnType;
		switch (columnType) {
		case DictionaryField: {
			interval.minVal = ((ValueState) ((DictionaryField) column.distribution).values.values().toArray()[0]).value;
			interval.maxVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = true;
		}
			break;
		case DistributedField: {
			interval.minVal = ((DistributedField) column.distribution).minValue;
			interval.maxVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = true;
		}
			break;
		case DistributedFieldWithEmissions: {
			interval.minVal = ((DistributedFieldWithEmissions) column.distribution).minValue;
			for (ValueState emission : ((DistributedFieldWithEmissions) column.distribution).values.values()) {
				Double emissionVal = Double.parseDouble(emission.value.toString());
				if ((Double) interval.minVal > emissionVal)
					interval.minVal = emissionVal;
			}
			interval.maxVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = true;
		}
			break;
		case Identificator: {
			interval.minVal = ((Identificator) column.distribution).minValue;
			interval.maxVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = true;
		}
			break;
		case NonNumericidentifier: {
			interval.maxVal = 1;
			interval.minVal = 1;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = true;
		}
			break;
		}
		return interval;
	}

	public static Interval getTntervalForLt(Object val, Column column) {
		Interval interval = new Interval();
		GlobalColumnType columnType = column.globalColumnType;
		switch (columnType) {
		case DictionaryField: {
			interval.minVal = ((ValueState) ((DictionaryField) column.distribution).values.values().toArray()[0]).value;
			interval.maxVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = false;
		}
			break;
		case DistributedField: {
			interval.minVal = ((DistributedField) column.distribution).minValue;
			interval.maxVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = false;
		}
			break;
		case DistributedFieldWithEmissions: {
			interval.minVal = ((DistributedFieldWithEmissions) column.distribution).minValue;
			for (ValueState emission : ((DistributedFieldWithEmissions) column.distribution).values.values()) {
				Double emissionVal = Double.parseDouble(emission.value.toString());
				if ((Double) interval.minVal > emissionVal)
					interval.minVal = emissionVal;
			}
			interval.maxVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = false;
		}
			break;
		case Identificator: {
			interval.minVal = ((Identificator) column.distribution).minValue;
			interval.maxVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = false;
		}
			break;
		}
		return interval;
	}

	public static Interval getTntervalForGt(Object val, Column column) {
		Interval interval = new Interval();
		GlobalColumnType columnType = column.globalColumnType;
		switch (columnType) {
		case DictionaryField: {
			int valuesCount = ((DictionaryField) column.distribution).values.values().size();
			interval.maxVal = ((ValueState) ((DictionaryField) column.distribution).values.values()
					.toArray()[valuesCount - 1]).value;
			interval.minVal = val;
			interval.strictMinBorder = false;
			interval.strictMaxBorder = true;
		}
			break;
		case DistributedField: {
			interval.maxVal = ((DistributedField) column.distribution).maxValue;
			interval.minVal = val;
			interval.strictMinBorder = false;
			interval.strictMaxBorder = true;
		}
			break;
		case DistributedFieldWithEmissions: {
			interval.maxVal = ((DistributedFieldWithEmissions) column.distribution).maxValue;
			for (ValueState emission : ((DistributedFieldWithEmissions) column.distribution).values.values()) {
				Double emissionVal = Double.parseDouble(emission.value.toString());
				if ((Double) interval.maxVal < emissionVal)
					interval.maxVal = emissionVal;
			}
			interval.minVal = val;
			interval.strictMinBorder = false;
			interval.strictMaxBorder = true;
		}
			break;
		case Identificator: {
			interval.maxVal = ((Identificator) column.distribution).maxValue;
			interval.minVal = val;
			interval.strictMinBorder = false;
			interval.strictMaxBorder = true;
		}
			break;
		}
		return interval;
	}

	public static Interval getTntervalForGe(Object val, Column column) {
		Interval interval = new Interval();
		GlobalColumnType columnType = column.globalColumnType;
		switch (columnType) {
		case DictionaryField: {
			int valuesCount = ((DictionaryField) column.distribution).values.values().size();
			interval.maxVal = ((ValueState) ((DictionaryField) column.distribution).values.values()
					.toArray()[valuesCount - 1]).value;
			interval.minVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = true;
		}
			break;
		case DistributedField: {
			interval.maxVal = ((DistributedField) column.distribution).maxValue;
			interval.minVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = true;
		}
			break;
		case DistributedFieldWithEmissions: {
			interval.maxVal = ((DistributedFieldWithEmissions) column.distribution).maxValue;
			for (ValueState emission : ((DistributedFieldWithEmissions) column.distribution).values.values()) {
				Double emissionVal = Double.parseDouble(emission.value.toString());
				if ((Double) interval.maxVal < emissionVal)
					interval.maxVal = emissionVal;
			}
			interval.minVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = true;
		}
			break;
		case Identificator: {
			interval.maxVal = ((Identificator) column.distribution).maxValue;
			interval.minVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = true;
		}
			break;
		case NonNumericidentifier: {
			interval.maxVal = 1;
			interval.minVal = 1;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = true;
		}
			break;
		}
		return interval;
	}

	public static List<Interval> getTntervalForNe(Object val, Column column) {
		List<Interval> intervals = new ArrayList<Interval>();

		GlobalColumnType columnType = column.globalColumnType;
		Interval interval = new Interval();
		Interval interval2 = new Interval();
		switch (columnType) {
		case DictionaryField: {
			int valuesCount = ((DictionaryField) column.distribution).values.values().size();
			interval.minVal = ((ValueState) ((DictionaryField) column.distribution).values.values().toArray()[0]).value;
			interval.maxVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = false;

			interval2.minVal = val;
			interval2.maxVal = ((ValueState) ((DictionaryField) column.distribution).values.values()
					.toArray()[valuesCount - 1]).value;
			interval2.strictMinBorder = false;
			interval2.strictMaxBorder = true;
		}
			break;
		case DistributedField: {
			interval.minVal = ((DistributedField) column.distribution).minValue;
			interval.maxVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = false;

			interval2.minVal = val;
			interval2.maxVal = ((DistributedField) column.distribution).maxValue;
			interval2.strictMinBorder = false;
			interval2.strictMaxBorder = true;
		}
			break;
		case DistributedFieldWithEmissions: {
			interval.minVal = ((DistributedFieldWithEmissions) column.distribution).minValue;
			for (ValueState emission : ((DistributedFieldWithEmissions) column.distribution).values.values()) {
				Double emissionVal = Double.parseDouble(emission.value.toString());
				if ((Double) interval.minVal > emissionVal)
					interval.minVal = emissionVal;
			}
			interval.maxVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = false;

			interval2.maxVal = ((DistributedFieldWithEmissions) column.distribution).maxValue;
			for (ValueState emission : ((DistributedFieldWithEmissions) column.distribution).values.values()) {
				Double emissionVal = Double.parseDouble(emission.value.toString());
				if ((Double) interval2.maxVal < emissionVal)
					interval2.maxVal = emission.value;
			}
			interval2.minVal = val;
			interval2.strictMinBorder = false;
			interval2.strictMaxBorder = true;
		}
			break;
		case Identificator: {
			interval.minVal = ((Identificator) column.distribution).minValue;
			interval.maxVal = val;
			interval.strictMinBorder = true;
			interval.strictMaxBorder = false;

			interval2.minVal = val;
			interval2.maxVal = ((Identificator) column.distribution).maxValue;
			interval2.strictMinBorder = false;
			interval2.strictMaxBorder = true;
		}
			break;
		}
		intervals.add(interval);
		intervals.add(interval2);
		return intervals;
	}

	public static List<Interval> getIntervalForPredicate(Operator op, Object val, Column column, Query q, Options opt,
			Table t) {
		List<Interval> intervals = new ArrayList<Interval>();

		switch (op) {
		case EQ: {

			Interval interval = getTntervalForEq(val, column, q, opt, t);
			if (interval != null)
				intervals.add(interval);
		}
			break;
		case LT: {
			Interval interval = getTntervalForLt(val, column);
			intervals.add(interval);
		}
			break;
		case GT: {
			Interval interval = getTntervalForGt(val, column);
			intervals.add(interval);
		}
			break;
		case LE: {
			Interval interval = getTntervalForLe(val, column);
			intervals.add(interval);
		}
			break;
		case GE: {
			Interval interval = getTntervalForGe(val, column);
			intervals.add(interval);
		}
			break;
		case NE: {
			intervals = getTntervalForNe(val, column);
		}
			break;
		}
		return intervals;
	}

	public static List<Interval> mergeIntervalsByDisjunctionForDictionaryField(List<Interval> originalIntervals,
			Column column) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		SortedMap<String, Point> points = new TreeMap<String, Point>();
		for (Interval interval : originalIntervals) {
			if (!interval.hasOnlyIntervalsEstimatedRowCount) {
				try {
					Point p = new Point();
					p.value = interval.minVal;
					p.isMinBorder = true;
					p.isStrict = interval.strictMinBorder;
					points.put((String) interval.minVal, p);

					Point p2 = new Point();
					p2.value = (String) interval.maxVal;
					p2.isMinBorder = false;
					p2.isStrict = interval.strictMaxBorder;
					if (points.containsKey((String) interval.maxVal)) {
						resIntervals.add(interval);
					} else {
						points.put((String) interval.maxVal, p2);
					}
				} catch (Exception e) {
					String exstr = e.toString();
				}
			} else
				resIntervals.add(interval);
		}

		List<Interval> r = getFinalIntervalsForDisjunction(points);
		for (Interval intl : r) {
			resIntervals.add(intl);
		}
		return resIntervals;
	}

	public static List<Interval> mergeIntervalsByDisjunctionForDistributedField(List<Interval> originalIntervals,
			Column column) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		SortedMap<Double, Point> points = new TreeMap<Double, Point>();
		for (Interval interval : originalIntervals) {
			if (!interval.hasOnlyIntervalsEstimatedRowCount) {
				try {
					Point p = new Point();
					Double val = Double.parseDouble(interval.minVal.toString().replace("'", ""));
					p.value = val;
					p.isMinBorder = true;
					p.isStrict = interval.strictMinBorder;
					points.put(val, p);

					Point p2 = new Point();
					val = Double.parseDouble(interval.maxVal.toString().replace("'", ""));
					p2.value = val;
					p2.isMinBorder = false;
					p2.isStrict = interval.strictMaxBorder;
					points.put(val, p2);
				} catch (Exception ex) {
					///// System.out.println("column.Name = " + column.Name + ";
					///// ex = " + ex);
					return null;
				}
			} else
				resIntervals.add(interval);
		}

		List<Interval> r = getFinalIntervalsForDisjunction(points);
		for (Interval intl : r) {
			resIntervals.add(intl);
		}
		return resIntervals;
	}

	public static List<Interval> mergeIntervalsByDisjunctionForIdentificator(List<Interval> originalIntervals,
			Column column) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		SortedMap<Long, Point> points = new TreeMap<Long, Point>();
		for (Interval interval : originalIntervals) {
			if (!interval.hasOnlyIntervalsEstimatedRowCount) {
				Point p = new Point();

				Long val = new Long(0);
				if (interval.minVal.toString().contains("x"))
					val = Long.parseLong(interval.minVal.toString().replace("0x", "").replace("x", ""), 16);
				else {
					try {
						val = Long.parseLong(interval.minVal.toString());
					} catch (Exception ex) {
						///// System.out.println("column.Name = " + column.Name
						///// + "; ex = " + ex);
						return null;
					}
				}
				p.value = val;
				p.isMinBorder = true;
				p.isStrict = interval.strictMinBorder;
				points.put(val, p);

				Point p2 = new Point();
				if (interval.maxVal.toString().contains("x"))
					val = Long.parseLong(interval.maxVal.toString().replace("0x", "").replace("x", ""), 16);
				else {
					try {
						val = Long.parseLong(interval.maxVal.toString());
					} catch (Exception ex) {
						// System.out.println("column.Name = " + column.Name +
						// "; ex = " + ex);
						return null;
					}
				}
				p2.value = val;
				p2.isMinBorder = false;
				p2.isStrict = interval.strictMaxBorder;
				if (points.containsKey(val))
					resIntervals.add(interval);
				else
					points.put(val, p2);
			} else
				resIntervals.add(interval);
		}

		List<Interval> r = getFinalIntervalsForDisjunction(points);
		for (Interval intl : r) {
			resIntervals.add(intl);
		}
		return resIntervals;
	}

	public static <T> List<Interval> getFinalIntervalsForDisjunction(SortedMap<T, Point> points) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		Boolean isStart = true;
		Interval intl = new Interval();
		Integer iInt = 0;
		// now we have sorted points
		for (Point p : points.values()) {
			if (isStart) {
				intl = new Interval();
				intl.minVal = p.value;
				intl.strictMinBorder = p.isStrict;
				// iInt++;
			}
			isStart = false;

			if (p.isMinBorder)
				iInt++;
			else
				iInt--;

			if (iInt <= 0) {
				intl.maxVal = p.value;
				intl.strictMaxBorder = p.isStrict;

				Interval intl2 = new Interval();
				intl2.maxVal = intl.maxVal;
				intl2.minVal = intl.minVal;
				intl2.strictMaxBorder = intl.strictMaxBorder;
				intl2.strictMinBorder = intl.strictMinBorder;
				resIntervals.add(intl2);
				isStart = true;
			}

		}
		return resIntervals;
	}

	public static List<Interval> mergeIntervalsByDisjunction(List<Interval> originalIntervals, Column column) {
		Object points = null;
		List<Interval> resIntervals = new ArrayList<Interval>();
		points = new TreeMap<String, Point>();
		switch (column.globalColumnType) {
		case DictionaryField: {
			resIntervals = mergeIntervalsByDisjunctionForDictionaryField(originalIntervals, column);
		}
			break;
		case DistributedField: {
			resIntervals = mergeIntervalsByDisjunctionForDistributedField(originalIntervals, column);
		}
			break;
		case DistributedFieldWithEmissions: {
			resIntervals = mergeIntervalsByDisjunctionForDistributedField(originalIntervals, column);
		}
			break;
		case Identificator: {
			resIntervals = mergeIntervalsByDisjunctionForIdentificator(originalIntervals, column);
		}
			break;
		}
		return resIntervals;
	}

	public static List<Interval> mergeIntervalsByConjunction(List<List<Interval>> originalIntervals, Column column) {
		Object points = null;
		List<Interval> resIntervals = new ArrayList<Interval>();
		points = new TreeMap<String, Point>();
		switch (column.globalColumnType) {
		case DictionaryField: {
			resIntervals = mergeIntervalsByConjunctionForDictionaryField(originalIntervals, column);
		}
			break;
		case DistributedField: {
			resIntervals = mergeIntervalsByConjunctionForDistributedField(originalIntervals, column);
		}
			break;
		case DistributedFieldWithEmissions: {
			resIntervals = mergeIntervalsByConjunctionForDistributedField(originalIntervals, column);
		}
			break;
		case Identificator: {
			resIntervals = mergeIntervalsByConjunctionForIdentificator(originalIntervals, column);
		}
			break;
		}
		return resIntervals;
	}

	public static <T> List<Interval> getFinalIntervalsForConjunction(List<List<Interval>> originalIntervals,
			SortedMap<T, List<Point>> points) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		Interval intl = new Interval();
		int needHasCount = originalIntervals.size();
		List<String> has = new ArrayList<String>();
		// now we have sorted points
		for (List<Point> point : points.values()) {
			for (Point p : point) {

				if (p.isMinBorder) {
					has.add(p.index.toString());
					if (intl.maxVal != null) {
						if (intl.maxVal.equals(p.value))
							has.add(intl.index);
					}
					intl = new Interval();
					intl.minVal = p.value;
					intl.index = p.index.toString();
					intl.strictMinBorder = p.isStrict;
				} else {
					intl.maxVal = p.value;
					intl.strictMaxBorder = p.isStrict;
					if (has.size() == needHasCount) {

						Interval intl2 = new Interval();
						intl2.maxVal = intl.maxVal;
						intl2.minVal = intl.minVal;
						intl2.strictMaxBorder = intl.strictMaxBorder;
						intl2.strictMinBorder = intl.strictMinBorder;
						resIntervals.add(intl2);

					}
					has.remove(p.index.toString());
				}
			}
		}
		return resIntervals;
	}

	public static List<Interval> mergeIntervalsByConjunctionForIdentificator(List<List<Interval>> originalIntervals,
			Column column) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		SortedMap<Long, List<Point>> points = new TreeMap<Long, List<Point>>();

		int i = 0;
		for (List<Interval> listIntervals : originalIntervals) {
			for (Interval interval : listIntervals) {
				if (!interval.hasOnlyIntervalsEstimatedRowCount) {
					Point p = new Point();

					Long minVal = new Long(0);
					Long maxVal = new Long(0);

					if (interval.minVal.toString().contains("x")) {
						minVal = Long.parseLong(interval.minVal.toString().replace("0x", "").replace("x", ""), 16);
						maxVal = Long.parseLong(interval.maxVal.toString().replace("0x", "").replace("x", ""), 16);
					} else {
						minVal = Long.parseLong(interval.minVal.toString());
						maxVal = Long.parseLong(interval.maxVal.toString());
					}

					p.value = minVal;
					p.isMinBorder = true;
					p.isStrict = interval.strictMinBorder;
					p.index = i;
					if (!points.containsKey(minVal)) {
						List<Point> point = new ArrayList<Point>();
						point.add(p);
						points.put((Long) minVal, point);
					} else
						points.get((Long) minVal).add(p);

					Point p2 = new Point();
					p2.value = (Long) maxVal;
					p2.isMinBorder = false;
					p2.isStrict = interval.strictMaxBorder;
					p2.index = i;
					if (!points.containsKey(maxVal)) {
						List<Point> point = new ArrayList<Point>();
						point.add(p2);
						points.put((Long) maxVal, point);
					} else
						points.get((Long) maxVal).add(p2);
				}
			}
			i++;
		}

		List<Interval> r = getFinalIntervalsForConjunction(originalIntervals, points);
		for (Interval intl : r) {
			resIntervals.add(intl);
		}
		return resIntervals;
	}

	public static List<Interval> mergeIntervalsByConjunctionForDistributedField(List<List<Interval>> originalIntervals,
			Column column) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		SortedMap<Double, List<Point>> points = new TreeMap<Double, List<Point>>();

		int i = 0;
		for (List<Interval> listIntervals : originalIntervals) {
			for (Interval interval : listIntervals) {
				if (!interval.hasOnlyIntervalsEstimatedRowCount) {
					Point p = new Point();
					p.value = (Double) interval.minVal;
					p.isMinBorder = true;
					p.isStrict = interval.strictMinBorder;
					p.index = i;
					if (!points.containsKey(interval.minVal)) {
						List<Point> point = new ArrayList<Point>();
						point.add(p);
						points.put((Double) interval.minVal, point);
					} else
						points.get((Double) interval.minVal).add(p);

					Point p2 = new Point();
					p2.value = (Double) interval.maxVal;
					p2.isMinBorder = false;
					p2.isStrict = interval.strictMaxBorder;
					p2.index = i;
					if (!points.containsKey(interval.maxVal)) {
						List<Point> point = new ArrayList<Point>();
						point.add(p2);
						points.put((Double) interval.maxVal, point);
					} else
						points.get((Double) interval.maxVal).add(p2);
				}

			}
			i++;
		}

		List<Interval> r = getFinalIntervalsForConjunction(originalIntervals, points);
		for (Interval intl : r) {
			resIntervals.add(intl);
		}
		return resIntervals;
	}

	public static List<Interval> mergeIntervalsByConjunctionForDictionaryField(List<List<Interval>> originalIntervals,
			Column column) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		SortedMap<String, List<Point>> points = new TreeMap<String, List<Point>>();

		int i = 0;
		for (List<Interval> listIntervals : originalIntervals) {
			for (Interval interval : listIntervals) {
				if (!interval.hasOnlyIntervalsEstimatedRowCount) {
					Point p = new Point();
					p.value = (String) interval.minVal;
					p.isMinBorder = true;
					p.isStrict = interval.strictMinBorder;
					p.index = i;
					if (!points.containsKey(interval.minVal)) {
						List<Point> point = new ArrayList<Point>();
						point.add(p);
						points.put((String) interval.minVal, point);
					} else
						points.get((String) interval.minVal).add(p);

					Point p2 = new Point();
					p2.value = (String) interval.maxVal;
					p2.isMinBorder = false;
					p2.isStrict = interval.strictMaxBorder;
					p2.index = i;
					if (!points.containsKey(interval.maxVal)) {
						List<Point> point = new ArrayList<Point>();
						point.add(p2);
						points.put((String) interval.maxVal, point);
					} else
						points.get((String) interval.maxVal).add(p2);
				}

			}
			i++;
		}

		List<Interval> r = getFinalIntervalsForConjunction(originalIntervals, points);
		for (Interval intl : r) {
			resIntervals.add(intl);
		}
		return resIntervals;
	}

	public static void writeIntervalForQureryAndColumn(Long statId, Query q1, Table t, Options opt, Connection conn) {
		Map<String, List<Interval>> res = new TreeMap<String, List<Interval>>();
		// Map<String, Map<Integer, List<Predicate>>> columnsListpred1 =
		// getPredicateForEachColumn(q1, t, opt);
		Map<String, List<Interval>> mapColIntervals1 = getIntervalForQureryAndColumn(q1, t, opt);

		Set<String> columnsInQuery1_ = mapColIntervals1.keySet();

		ArrayList<String> columnsInQuery1 = new ArrayList<String>();
		columnsInQuery1.addAll(columnsInQuery1_);

		Map<String, List<Interval>> mapColIntervalsAll = new TreeMap<String, List<Interval>>();
		Long[] attributeBitmap = new Long[24];
		attributeBitmap[0] = new Long(0);
		attributeBitmap[1] = new Long(0);
		attributeBitmap[2] = new Long(0);
		attributeBitmap[3] = new Long(0);
		attributeBitmap[4] = new Long(0);
		attributeBitmap[5] = new Long(0);
		attributeBitmap[6] = new Long(0);
		attributeBitmap[7] = new Long(0);
		attributeBitmap[8] = new Long(0);
		attributeBitmap[9] = new Long(0);
		attributeBitmap[10] = new Long(0);
		attributeBitmap[11] = new Long(0);
		attributeBitmap[12] = new Long(0);
		attributeBitmap[13] = new Long(0);
		attributeBitmap[14] = new Long(0);
		attributeBitmap[15] = new Long(0);
		attributeBitmap[16] = new Long(0);
		attributeBitmap[17] = new Long(0);
		attributeBitmap[18] = new Long(0);
		attributeBitmap[19] = new Long(0);
		attributeBitmap[20] = new Long(0);
		attributeBitmap[21] = new Long(0);
		attributeBitmap[22] = new Long(0);
		attributeBitmap[23] = new Long(0);

		for (String column : columnsInQuery1) {
			List<Interval> intervals = mapColIntervals1.get(column);
			Column c = opt.COLUMNS_DISTRIBUTION.get(column);
			Long tmp = new Long(0);
			if (intervals.size() != 0) {
				attributeBitmap = SetAttributeMask(attributeBitmap, c.attributeId);

				String columnName = c.name.toLowerCase();
				Pair<Integer, Integer> attrIdAnddataType = t.columns.get(columnName);
				writeIntervalsToDB(statId, t, c, intervals, conn, attrIdAnddataType);
			}

		}
		if ((attributeBitmap[0] != 0) || (attributeBitmap[1] != 0) || (attributeBitmap[2] != 0)
				|| (attributeBitmap[3] != 0) || (attributeBitmap[4] != 0) || (attributeBitmap[5] != 0)
				|| (attributeBitmap[6] != 0) || (attributeBitmap[7] != 0) || (attributeBitmap[8] != 0)
				|| (attributeBitmap[9] != 0) || (attributeBitmap[10] != 0) || (attributeBitmap[11] != 0)
				|| (attributeBitmap[12] != 0) || (attributeBitmap[13] != 0) || (attributeBitmap[14] != 0)
				|| (attributeBitmap[15] != 0) || (attributeBitmap[16] != 0) || (attributeBitmap[17] != 0)
				|| (attributeBitmap[18] != 0) || (attributeBitmap[19] != 0) || (attributeBitmap[20] != 0)
				|| (attributeBitmap[21] != 0) || (attributeBitmap[22] != 0) || (attributeBitmap[23] != 0))
			writeAttributeBitmask(attributeBitmap, conn);
		return;// res;
	}

	public static Long[] SetAttributeMask(Long[] attrMask, Integer attributeId) {
		int group = attributeId / 63;
		int indexInGroup = attributeId % 63;
		attrMask[group] |= Math.round(Math.pow(2, indexInGroup));
		return attrMask;
	}

	public static void writeIntervalsToDB(Long statId, Table t, Column c, List<Interval> intervalsInQuery1,
			Connection conn, Pair<Integer, Integer> attrIdAnddataType) {
		// public final static Integer typeLong = 1;
		// public final static Integer typeFloat = 2;
		// public final static Integer typeString = 3;
		// public final static Integer typeDatetime = 4;

		switch (attrIdAnddataType.getSecond()) {
		case 1:
			writeLongInterval(statId, t, c, intervalsInQuery1, conn, attrIdAnddataType);
			break;
		case 2:
			writeFloatInterval(statId, t, c, intervalsInQuery1, conn, attrIdAnddataType);
			break;
		case 3:
			writeStringInterval(statId, t, c, intervalsInQuery1, conn, attrIdAnddataType);
			break;
		case 4:
			writeDateInterval(statId, t, c, intervalsInQuery1, conn, attrIdAnddataType);
			break;
		}

	}

	private static void writeDateInterval(Long statId, Table t, Column c, List<Interval> intervalsInQuery1,
			Connection conn, Pair<Integer, Integer> attrIdAnddataType) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			ResultSet rs = null;

			for (Interval intl : intervalsInQuery1) {

				String query = "MERGE INTO QRS_STAT_ATTR_DATETIME t " + "USING (SELECT " + statId + " as STAT_ID, "
						+ c.attributeId + " as ATTR_ID  from dual) h "
						+ "ON (h.STAT_ID = t.STAT_ID and h.ATTR_ID = t.ATTR_ID) " + "WHEN NOT MATCHED THEN "
						+ "INSERT (STAT_ID,	ATTR_ID, MIN_VAL, MAX_VAL) VALUES (" + statId + "," + c.attributeId + ", "
						+ (String) intl.minVal + ", " + (String) intl.maxVal + ")";

				rs = st.executeQuery(query);
				// Write the interval

			}
		} catch (Exception ex) {

		}
	}

	private static void writeStringInterval(Long statId, Table t, Column c, List<Interval> intervalsInQuery1,
			Connection conn, Pair<Integer, Integer> attrIdAnddataType) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			ResultSet rs = null;

			for (Interval intl : intervalsInQuery1) {
				String query = "MERGE INTO QRS_STAT_ATTR_STRING t " + "USING (SELECT " + statId + " as STAT_ID, "
						+ c.attributeId + " as ATTR_ID from dual) h "
						+ "ON (h.STAT_ID = t.STAT_ID and h.ATTR_ID = t.ATTR_ID) " + "WHEN NOT MATCHED THEN "
						+ "INSERT (STAT_ID,	ATTR_ID, MIN_VAL, MAX_VAL) VALUES (" + statId + "," + c.attributeId + ", "
						+ (String) intl.minVal + ", " + (String) intl.maxVal + ")";

				rs = st.executeQuery(query);
				// Write the interval

			}
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}

	private static void writeAttributeBitmask(Long[] attributeMask, Connection conn) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			ResultSet rs = null;

			String query = "MERGE INTO QRS_ATTR_OCCURANCE t " + "USING (SELECT " + attributeMask[0]
					+ " as ATTR_MASK_0, " + attributeMask[1] + " as ATTR_MASK_1, " + attributeMask[2]
					+ " as ATTR_MASK_2, " + attributeMask[3] + " as ATTR_MASK_3,  " + attributeMask[4]
					+ " as ATTR_MASK_4, " + attributeMask[5] + " as ATTR_MASK_5, " + attributeMask[6]
					+ " as ATTR_MASK_6, " + attributeMask[7] + " as ATTR_MASK_7, " + attributeMask[8]
					+ " as ATTR_MASK_8, " + attributeMask[9] + " as ATTR_MASK_9, " + attributeMask[10]
					+ " as ATTR_MASK_10, " + attributeMask[11] + " as ATTR_MASK_11, " + attributeMask[12]
					+ " as ATTR_MASK_12, " + attributeMask[13] + " as ATTR_MASK_13, " + attributeMask[14]
					+ " as ATTR_MASK_14, " + attributeMask[15] + " as ATTR_MASK_15, " + attributeMask[16]
					+ " as ATTR_MASK_16, " + attributeMask[17] + " as ATTR_MASK_17, " + attributeMask[18]
					+ " as ATTR_MASK_18, " + attributeMask[19] + " as ATTR_MASK_19, " + attributeMask[20]
					+ " as ATTR_MASK_20, " + attributeMask[21] + " as ATTR_MASK_21, " + attributeMask[22]
					+ " as ATTR_MASK_22, " + attributeMask[23] + " as ATTR_MASK_23 "
					+ "from dual) h ON (h.ATTR_MASK_0 = t.ATTR_MASK_0 and h.ATTR_MASK_1 = t.ATTR_MASK_1 and h.ATTR_MASK_2 = t.ATTR_MASK_2 and h.ATTR_MASK_3 = t.ATTR_MASK_3 and "
					+ "h.ATTR_MASK_4 = t.ATTR_MASK_4 and h.ATTR_MASK_5 = t.ATTR_MASK_5 and h.ATTR_MASK_6 = t.ATTR_MASK_6 and h.ATTR_MASK_7 = t.ATTR_MASK_7 and "
					+ "h.ATTR_MASK_8 = t.ATTR_MASK_8 and h.ATTR_MASK_9 = t.ATTR_MASK_9 and h.ATTR_MASK_10 = t.ATTR_MASK_10 and h.ATTR_MASK_11 = t.ATTR_MASK_11 and "
					+ "h.ATTR_MASK_12 = t.ATTR_MASK_12 and h.ATTR_MASK_13 = t.ATTR_MASK_13 and h.ATTR_MASK_14 = t.ATTR_MASK_14 and h.ATTR_MASK_15 = t.ATTR_MASK_15 and "
					+ "h.ATTR_MASK_16 = t.ATTR_MASK_16 and h.ATTR_MASK_17 = t.ATTR_MASK_17 and h.ATTR_MASK_18 = t.ATTR_MASK_18 and h.ATTR_MASK_19 = t.ATTR_MASK_19 and "
					+ "h.ATTR_MASK_20 = t.ATTR_MASK_20 and h.ATTR_MASK_21 = t.ATTR_MASK_21 and h.ATTR_MASK_22 = t.ATTR_MASK_22 and h.ATTR_MASK_23 = t.ATTR_MASK_23) "
					+ " WHEN MATCHED THEN    UPDATE SET t.COUNT = t.COUNT + 1 " + "WHEN NOT MATCHED THEN "
					+ "INSERT (ATTR_MASK_0,  ATTR_MASK_1,  ATTR_MASK_2,  ATTR_MASK_3, ATTR_MASK_4,  ATTR_MASK_5,  ATTR_MASK_6,  ATTR_MASK_7, ATTR_MASK_8,  ATTR_MASK_9,  ATTR_MASK_10,  ATTR_MASK_11, ATTR_MASK_12,  ATTR_MASK_13,  ATTR_MASK_14,  ATTR_MASK_15, ATTR_MASK_16,  ATTR_MASK_17,  ATTR_MASK_18,  ATTR_MASK_19, ATTR_MASK_20,  ATTR_MASK_21,  ATTR_MASK_22,  ATTR_MASK_23, COUNT,  ATTR_MASK_ID) VALUES ("
					+ attributeMask[0] + "," + attributeMask[1] + ", " + attributeMask[2] + ", " + attributeMask[3]
					+ ", " + attributeMask[4] + ", " + attributeMask[5] + ", " + attributeMask[6] + ", "
					+ attributeMask[7] + "," + +attributeMask[8] + "," + attributeMask[9] + ", " + attributeMask[10]
					+ ", " + attributeMask[11] + ", " + attributeMask[12] + "," + attributeMask[13] + ", "
					+ attributeMask[14] + ", " + attributeMask[15] + ", " + +attributeMask[16] + "," + attributeMask[17]
					+ ", " + attributeMask[18] + ", " + attributeMask[19] + ", " + attributeMask[20] + ","
					+ attributeMask[21] + ", " + attributeMask[22] + ", " + attributeMask[23] + ", 1, "
					+ "NVL((SELECT MAX(ATTR_MASK_ID) + 1 FROM QRS_ATTR_OCCURANCE),1))";

			rs = st.executeQuery(query);
			// Write the interval

		} catch (Exception ex) {
			System.out.println(ex);
		}
	}

	private static void writeFloatInterval(Long statId, Table t, Column c, List<Interval> intervalsInQuery1,
			Connection conn, Pair<Integer, Integer> attrIdAnddataType) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			ResultSet rs = null;

			for (Interval intl : intervalsInQuery1) {
				String query = "MERGE INTO QRS_STAT_ATTR_FLOAT t " + "USING (SELECT " + statId + " as STAT_ID, "
						+ c.attributeId + " as ATTR_ID from dual) h "
						+ "ON (h.STAT_ID = t.STAT_ID and h.ATTR_ID = t.ATTR_ID) " + "WHEN NOT MATCHED THEN "
						+ "INSERT (STAT_ID,	ATTR_ID, MIN_VAL, MAX_VAL) VALUES (" + statId + "," + c.attributeId + ", "
						+ Double.parseDouble(intl.minVal.toString()) + ", " + Double.parseDouble(intl.maxVal.toString())
						+ ")";

				rs = st.executeQuery(query);
				// Write the interval

			}
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}

	private static void writeLongInterval(Long statId, Table t, Column c, List<Interval> intervalsInQuery1,
			Connection conn, Pair<Integer, Integer> attrIdAnddataType) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			ResultSet rs = null;

			for (Interval intl : intervalsInQuery1) {
				Long minVal = new Long(0);
				Long maxVal = new Long(0);
				if (intl.minVal.toString().contains(".")) {
					minVal = (long) Double.parseDouble(intl.minVal.toString());
					maxVal = (long) Double.parseDouble(intl.maxVal.toString());
				} else {
					if (intl.minVal.toString().contains("x")) {
						minVal = Long.parseLong(intl.minVal.toString().replace("0x", "").replace("x", ""), 16);
						maxVal = Long.parseLong(intl.maxVal.toString().replace("0x", "").replace("x", ""), 16);
					} else {
						minVal = Long.parseLong(intl.minVal.toString());
						maxVal = Long.parseLong(intl.maxVal.toString());
					}
				}
				String query = "MERGE INTO QRS_STAT_ATTR_NUMBER t " + "USING (SELECT " + statId + " as STAT_ID, "
						+ c.attributeId + " as ATTR_ID  from dual) h "
						+ "ON (h.STAT_ID = t.STAT_ID and h.ATTR_ID = t.ATTR_ID) " + "WHEN NOT MATCHED THEN "
						+ "INSERT (STAT_ID,	ATTR_ID, MIN_VAL, MAX_VAL) VALUES (" + statId + "," + c.attributeId + ", "
						+ minVal + ", " + maxVal + ")";

				rs = st.executeQuery(query);
				// Write the interval

			}
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}

	public static Map<String, List<Interval>> getIntervalForQureryAndColumn(Query q1, Table t, Options opt) {
		Map<String, List<Interval>> res = new TreeMap<String, List<Interval>>();
		Map<String, Map<Integer, List<Predicate>>> columnsListpred1 = getPredicateForEachColumn(q1, t, opt);

		for (Map<Integer, List<Predicate>> columnWithPredicate : columnsListpred1.values()) {
			Column c = null;
			List<List<Interval>> intervalsAll = new ArrayList<List<Interval>>();
			for (List<Predicate> disjPred : columnWithPredicate.values()) {

				List<Interval> intervals = new ArrayList<Interval>();
				for (Predicate prediacte : disjPred) {
					Operator op = prediacte.op;
					Object val = prediacte.value.toLowerCase();

					String columnName = prediacte.table + "." + prediacte.column;
					// c is column, we are working with it now
					c = opt.COLUMNS_DISTRIBUTION.get(columnName);
					if (c == null) {
						// System.out.println("column = " + columnName + "is
						// null");
						return null;
					} else {
						List<Interval> tmp = getIntervalForPredicate(op, val, c, q1, opt, t);
						for (Interval intl : tmp) {
							intervals.add(intl);
						}
					}
				}
				if (c != null) {

					List<Interval> intervals2 = mergeIntervalsByDisjunction(intervals, c);
					if (intervals2 == null)
						return null;
					intervalsAll.add(intervals2);
				}

			}
			if (c != null) {
				List<Interval> intervalsInQuery1 = mergeIntervalsByConjunction(intervalsAll, c);

				res.put(c.name, intervalsInQuery1);
			}
		}
		return res;
	}

	public static long getEstimatedRowsOverall(Query q1, Query q2, Table t, Options opt) {
		if (t == null)
			return 0;
		long res1 = t.count;
		long res2 = t.count;

		Map<String, List<Interval>> mapColIntervals1 = getIntervalForQureryAndColumn(q1, t, opt);
		Map<String, List<Interval>> mapColIntervals2 = getIntervalForQureryAndColumn(q2, t, opt);

		if ((mapColIntervals1 == null) || (mapColIntervals2 == null))
			return -1;
		Set<String> columnsInQuery1_ = mapColIntervals1.keySet();
		Set<String> columnsInQuery2_ = mapColIntervals2.keySet();

		ArrayList<String> columnsInQuery1 = new ArrayList<String>();
		columnsInQuery1.addAll(columnsInQuery1_);
		ArrayList<String> columnsInQuery2 = new ArrayList<String>();
		columnsInQuery2.addAll(columnsInQuery2_);

		Map<String, List<Interval>> mapColIntervalsAll = new TreeMap<String, List<Interval>>();
		for (String column : columnsInQuery1) {
			List<Interval> intervals = mapColIntervals1.get(column);
			Column c = opt.COLUMNS_DISTRIBUTION.get(column);
			Long tmp = new Long(0);
			if (intervals.size() != 0)
				tmp = getEstimatedRowsCount(intervals, c, t);
			if (tmp < res1)
				res1 = tmp;
		}

		for (String column : columnsInQuery2) {
			List<Interval> intervals = mapColIntervals2.get(column);
			Column c = opt.COLUMNS_DISTRIBUTION.get(column);
			Long tmp = new Long(0);
			if (intervals.size() != 0)
				tmp = getEstimatedRowsCount(intervals, c, t);
			if (tmp < res2)
				res2 = tmp;
		}

		return res1 + res2;
	}

	public static Long getEstimatedRowsOverlap(Query q1, Query q2, Table t, Options opt) {
		if (t == null)
			return new Long(0);
		Long res = t.count;
		Double result = new Double(0);
		Map<String, List<Interval>> mapColIntervals1 = getIntervalForQureryAndColumn(q1, t, opt);
		Map<String, List<Interval>> mapColIntervals2 = getIntervalForQureryAndColumn(q2, t, opt);

		if ((mapColIntervals1 == null) || (mapColIntervals2 == null))
			return new Long(0);
		Set<String> columnsInQuery1_ = mapColIntervals1.keySet();
		Set<String> columnsInQuery2_ = mapColIntervals2.keySet();

		ArrayList<String> columnsInQuery1 = new ArrayList<String>();
		columnsInQuery1.addAll(columnsInQuery1_);
		ArrayList<String> columnsInQuery2 = new ArrayList<String>();
		columnsInQuery2.addAll(columnsInQuery2_);

		Map<String, List<Interval>> mapColIntervalsAll = new TreeMap<String, List<Interval>>();
		Map<String, List<Interval>> mapColIntervalsOnlyIn1 = new TreeMap<String, List<Interval>>();
		Map<String, List<Interval>> mapColIntervalsOnlyIn2 = new TreeMap<String, List<Interval>>();
		for (String column : columnsInQuery1) {

			if (columnsInQuery2.contains(column)) {
				List<List<Interval>> intervalAll = new ArrayList<List<Interval>>();
				List<Interval> intervals1 = mapColIntervals1.get(column);
				List<Interval> intervals2 = mapColIntervals2.get(column);
				intervalAll.add(intervals1);
				intervalAll.add(intervals2);
				Column c = opt.COLUMNS_DISTRIBUTION.get(column);
				List<Interval> intervalsall = mergeIntervalsByConjunction(intervalAll, c);
				mapColIntervalsAll.put(column, intervalsall);
			} else {
				List<Interval> intervals1 = mapColIntervals1.get(column);
				mapColIntervalsOnlyIn1.put(column, intervals1);
			}

		}

		for (String column : columnsInQuery2) {

			if (columnsInQuery1.contains(column)) {

			} else {
				List<Interval> intervals2 = mapColIntervals2.get(column);
				mapColIntervalsOnlyIn2.put(column, intervals2);
			}
		}

		Set<String> columnsInQueryAll_ = mapColIntervalsAll.keySet();

		ArrayList<String> columnsInQueryAll = new ArrayList<String>();
		columnsInQueryAll.addAll(columnsInQueryAll_);

		for (String column : columnsInQueryAll) {
			List<Interval> intervals = mapColIntervalsAll.get(column);
			Column c = opt.COLUMNS_DISTRIBUTION.get(column);
			Long tmp = new Long(0);
			if (intervals.size() != 0)
				tmp = getEstimatedRowsCount(intervals, c, t);
			if (tmp < res)
				res = tmp;
		}
		result = res.doubleValue() / t.count;

		Long res1 = t.count;
		Set<String> columnsInQueryOnlyIn1_ = mapColIntervalsOnlyIn1.keySet();

		ArrayList<String> columnsInQueryOnlyIn1 = new ArrayList<String>();
		columnsInQueryOnlyIn1.addAll(columnsInQueryOnlyIn1_);
		for (String column : columnsInQueryOnlyIn1) {
			List<Interval> intervals = mapColIntervalsOnlyIn1.get(column);
			Column c = opt.COLUMNS_DISTRIBUTION.get(column);
			Long tmp = new Long(0);
			if (intervals.size() != 0)
				tmp = getEstimatedRowsCount(intervals, c, t);
			if (tmp < res1)
				res1 = tmp;
		}

		Long res2 = t.count;
		Set<String> columnsInQueryOnlyIn2_ = mapColIntervalsOnlyIn2.keySet();

		ArrayList<String> columnsInQueryOnlyIn2 = new ArrayList<String>();
		columnsInQueryOnlyIn2.addAll(columnsInQueryOnlyIn2_);
		for (String column : columnsInQueryOnlyIn2) {
			List<Interval> intervals = mapColIntervalsOnlyIn2.get(column);
			Column c = opt.COLUMNS_DISTRIBUTION.get(column);
			Long tmp = new Long(0);
			if (intervals.size() != 0)
				tmp = getEstimatedRowsCount(intervals, c, t);
			if (tmp < res2)
				res2 = tmp;
		}

		Double result2 = (res1.doubleValue() * res2.doubleValue()) / (t.count * t.count);
		if (result2 < result)
			result = result2;
		Long finalRes = Math.round(result * t.count);
		return finalRes;
	}

	public static Long getEstimatedRowsCount(List<Interval> intervals, Column column, Table t) {
		Long res = new Long(0);
		Long res2 = t.count;
		// TODO: implementation
		for (Interval interval : intervals) {
			if (!interval.hasOnlyIntervalsEstimatedRowCount)
				// calculate the estimated row count inside the interval
				res = res + getEstimatedRowsCountInsideTheInterval(interval, column, t);
			else {
				Long r = getEstimatedRowsCountInsideTheInterval(interval, column, t);
				if (res2 > r)
					res2 = r;
			}
		}
		if (res == 0)
			res = t.count;
		return res < res2 ? res : res2;
	}

	public static Long getEstimatedRowsCountInsideTheIntervalForDictionaryField(Interval interval, Column column,
			Table t) {
		Long res = new Long(0);
		String minVal = interval.minVal.toString().replace("'", "");
		String maxVal = interval.maxVal.toString().replace("'", "");

		Boolean stricktMinBorder = interval.strictMinBorder;
		Boolean stricktMaxBorder = interval.strictMaxBorder;

		DictionaryField dictionaryField = (DictionaryField) column.distribution;
		ValueState minValueState = dictionaryField.values.get(minVal);
		ValueState maxValueState = dictionaryField.values.get(maxVal);

		if (minVal.equals(maxVal)) {
			if (minValueState == null) {
				res = new Long(0);
			} else
				res = minValueState.valuesCount;
		} else {
			Long minValCount = new Long(0);
			Long maxValCount = new Long(0);
			if (stricktMinBorder)
				minValCount = minValueState.valuesLessOrEqualCount - minValueState.valuesCount;
			else
				minValCount = minValueState.valuesLessOrEqualCount;

			if (stricktMaxBorder)
				maxValCount = maxValueState.valuesLessOrEqualCount;
			else
				maxValCount = maxValueState.valuesLessOrEqualCount - maxValueState.valuesCount;
			res = Math.abs(maxValCount - minValCount);

		}
		return res;
	}

	public static Long getEstimatedRowsCountInsideTheIntervalForDistributedField(Interval interval, Column column,
			Table t) {
		Long res = new Long(0);
		if (interval.hasOnlyIntervalsEstimatedRowCount)
			return interval.estimatedRowCount;
		Double minVal = (Double) interval.minVal;
		Double maxVal = (Double) interval.maxVal;

		DistributedField distributedField = (DistributedField) column.distribution;
		Double minValueState = distributedField.minValue;
		Double maxValueState = distributedField.maxValue;

		if (minVal.equals(maxVal)) {
			// this is a place to discuss
			res = new Long(1);
		} else {
			Double intervalWidth = (Double) Math.abs(maxVal - minVal);
			if (intervalWidth == 0)
				intervalWidth = (double) 1;
			Double columnWidth = (Double) Math.abs(maxValueState - minValueState);
			if (columnWidth == 0)
				columnWidth = (double) 1;
			res = Math.round(t.count * intervalWidth / columnWidth);
		}
		return res;
	}

	public static Long getEstimatedRowsCountInsideTheIntervalForDistributedFieldWithEmissions(Interval interval,
			Column column, Table t) {
		Long res = new Long(0);
		if (interval.hasOnlyIntervalsEstimatedRowCount)
			return interval.estimatedRowCount;
		Double minVal = (Double) interval.minVal;
		Double maxVal = (Double) interval.maxVal;

		Boolean stricktMinBorder = interval.strictMinBorder;
		Boolean stricktMaxBorder = interval.strictMaxBorder;

		DistributedFieldWithEmissions distributedFieldWithEmissions = (DistributedFieldWithEmissions) column.distribution;
		Double minValueState = distributedFieldWithEmissions.minValue;
		Double maxValueState = distributedFieldWithEmissions.maxValue;

		if (minVal.equals(maxVal)) {
			// this is a place to discuss
			res = new Long(1);
		} else {
			Double intervalWidth = (Double) Math.abs(maxVal - minVal);
			if (intervalWidth == 0)
				intervalWidth = (double) 1;
			Double columnWidth = (Double) Math.abs(maxValueState - minValueState);
			if (columnWidth == 0)
				columnWidth = (double) 1;
			res = Math.round(t.count * intervalWidth / columnWidth);
		}

		for (ValueState emission : distributedFieldWithEmissions.values.values()) {
			Double emissionVal = Double.parseDouble(emission.value.toString());
			if (stricktMinBorder && stricktMaxBorder) {
				if ((emissionVal >= minVal) && (emissionVal <= maxVal))
					res = res + emission.valuesCount;
			}
			if (stricktMinBorder && !stricktMaxBorder) {
				if ((emissionVal >= minVal) && (emissionVal < maxVal))
					res = res + emission.valuesCount;
			}
			if (!stricktMinBorder && stricktMaxBorder) {
				if ((emissionVal > minVal) && (emissionVal <= maxVal))
					res = res + emission.valuesCount;
			}
			if (!stricktMinBorder && !stricktMaxBorder) {
				if ((emissionVal >= minVal) && (emissionVal < maxVal))
					res = res + emission.valuesCount;
			}
		}
		return res;
	}

	public static Long getEstimatedRowsCountInsideTheIntervalForIdentificator(Interval interval, Column column,
			Table t) {
		Long res = new Long(0);
		if (interval.hasOnlyIntervalsEstimatedRowCount)
			return interval.estimatedRowCount;
		Long minVal = (Long) interval.minVal;
		Long maxVal = (Long) interval.maxVal;

		Identificator identificatorField = (Identificator) column.distribution;
		Long minValueState = identificatorField.minValue;
		Long maxValueState = identificatorField.maxValue;

		if (minVal.equals(maxVal)) {
			res = new Long(1);
		} else {
			Long intervalWidth = (Long) Math.abs(maxVal - minVal);
			if (intervalWidth == 0)
				intervalWidth = (long) 1;
			Long columnWidth = (Long) Math.abs(maxValueState - minValueState);
			if (columnWidth == 0)
				columnWidth = (long) 1;
			res = Math.round(t.count * (double) intervalWidth / columnWidth);
		}
		return res;
	}

	public static Long getEstimatedRowsCountInsideTheInterval(Interval interval, Column column, Table t) {
		Long res = new Long(0);

		switch (column.globalColumnType) {
		case DictionaryField: {
			res = getEstimatedRowsCountInsideTheIntervalForDictionaryField(interval, column, t);
		}
			break;
		case DistributedField: {
			res = getEstimatedRowsCountInsideTheIntervalForDistributedField(interval, column, t);
		}
			break;
		case DistributedFieldWithEmissions: {
			res = getEstimatedRowsCountInsideTheIntervalForDistributedFieldWithEmissions(interval, column, t);
		}
			break;
		case Identificator: {
			res = getEstimatedRowsCountInsideTheIntervalForIdentificator(interval, column, t);
		}
			break;
		case NonNumericidentifier: {
			res = new Long(1);
		}
			break;
		}
		return res;
	}

	public static Map<String, Map<Integer, List<Predicate>>> getPredicateForEachColumn(Query q, Table t, Options opt) {
		Map<String, Map<Integer, List<Predicate>>> res = new TreeMap<String, Map<Integer, List<Predicate>>>();
		List<List<Predicate>> predicatesInThisQuery = q.whereClausesTerms;
		long minimumRowCount = t.count;

		int iList = 0;
		for (List<Predicate> lispredicate : predicatesInThisQuery) {
			int i = 0;
			for (Predicate orPredicate : lispredicate) {
				String column = orPredicate.column;
				String table = orPredicate.table;
				String value = orPredicate.value;
				Operator operator = orPredicate.op;

				if (table.equals(t.name)) {
					String fullColumnName = table + "." + column;
					Column col = opt.COLUMNS_DISTRIBUTION.get(fullColumnName);
					Map<Integer, List<Predicate>> maplistPred = res.get(fullColumnName);
					if (maplistPred == null) {
						maplistPred = new TreeMap<Integer, List<Predicate>>();
						List<Predicate> listPred = new ArrayList<Predicate>();
						listPred.add(orPredicate);
						maplistPred.put(iList, listPred);
						res.put(fullColumnName, maplistPred);
					} else {
						List<Predicate> listPred = maplistPred.get(iList);
						if (listPred == null) {
							listPred = new ArrayList<Predicate>();
							listPred.add(orPredicate);
							maplistPred.put(iList, listPred);
						} else {
							listPred.add(orPredicate);
						}

					}
				}
			}
			iList++;
		}
		return res;
	}
}
