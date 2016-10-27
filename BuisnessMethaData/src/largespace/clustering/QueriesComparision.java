package largespace.clustering;

import java.io.BufferedWriter;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import accessarea.AccessArea;
import accessarea.AccessAreaExtraction;
import aima.core.util.datastructure.Pair;

import java.util.SortedMap;

import largespace.business.DataTypes;
import largespace.business.Operator;
import largespace.business.Options;
import largespace.business.RowInfo;
import largespace.business.Table;
import largespace.clustering.Column.GlobalColumnType;
import largespace.clustering.Interval;

public final class QueriesComparision {

	public static Interval GetTntervalForEq(Object val, Column column, Query q, Options opt, Table t) {
		Interval interval = new Interval();

		if (val.toString().contains(".")) {
			Boolean isNeedToCalculateInterval = IsNeedToCalculateInterval(val, opt, t);
			if (!isNeedToCalculateInterval)
				return null;
			// this is JOIN, we can't measure interval, only estimated intervals
			// row count
			interval.HasOnlyIntervalsEstimatedRowCount = true;
			Long estTCount = GetEstimetedRowCountInParentTable(column, t);
			interval.EstimatedRowCount = GetEstimetedRowCountInJoin(val, column, q, opt, estTCount);
		}
		interval.MinVal = val;
		interval.MaxVal = val;
		interval.StricktMinBorder = true;
		interval.StricktMaxBorder = true;

		return interval;
	}

	static HashMap<Integer, List<Column>> GetColumnsByType(ArrayList<String> columnsInQuery1, Options opt) {
		HashMap<Integer, List<Column>> columnsByType = new HashMap<Integer, List<Column>>();
		for (String column : columnsInQuery1) {
			Column c = opt.COLUMNS_DISTRIBUTION.get(column);
			switch (c.AttributeType) {
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

	public static Map<Long, Query> GetSimilarQueriesForQureryAndColumn(Long statId, Long lastUs, Query q1, Table t,
			Options opt, Connection conn, Map<Long, Query> res, BufferedWriter writer) {
		try {
			// Turn use of the cursor on.
			// Map<String, Map<Integer, List<Predicate>>> columnsListpred1 =
			// GetPredicateForEachColumn(q1, t, opt);
			Map<String, List<Interval>> mapColIntervals1 = GetIntervalForQureryAndColumn(q1, t, opt);

			Set<String> columnsInQuery1_ = mapColIntervals1.keySet();

			ArrayList<String> columnsInQuery1 = new ArrayList<String>();
			columnsInQuery1.addAll(columnsInQuery1_);
			HashMap<Integer, List<Column>> columnsByType = GetColumnsByType(columnsInQuery1, opt);

			for (String column : columnsInQuery1) {
				List<Interval> intervals = mapColIntervals1.get(column);
				Column c = opt.COLUMNS_DISTRIBUTION.get(column);
				Long tmp = new Long(0);
				if (intervals.size() != 0) {
					String columnName = c.Name.toLowerCase();
					Pair<Integer, Integer> attrIdAnddataType = t.Columns.get(columnName);
					res = GetQueriesWithSimilarIntervals(statId, lastUs, t, c, intervals, conn, attrIdAnddataType, opt,
							res, writer, columnsByType);

					if (res.size() == 0)
						res = GetQueriesWithSimilarAttributes(statId, lastUs, t, c, intervals, conn, attrIdAnddataType,
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
	private static Map<Long, Query> GetQueriesWithSimilarIntervalsNumber(Long statId, Long lastUs, Table t, Column c,
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
						theSetOfAttributes = theSetOfAttributes + c1.AttributeId + ",";
					}
					theSetOfAttributes = theSetOfAttributes.substring(0, theSetOfAttributes.length() - 1);
					Long val = new Long(0);
					if (intl.MinVal.toString().contains("x"))
						val = Long.parseLong(intl.MinVal.toString().replace("0x", "").replace("x", ""), 16);
					else {
						try {
							val = Long.parseLong(intl.MinVal.toString());
						} catch (Exception ex) {
							///// System.out.println("column.Name = " +
							///// column.Name + "; ex = " + ex);
							return null;
						}
					}
					minVal = val.toString();
					if (intl.MaxVal.toString().contains("x"))
						val = Long.parseLong(intl.MaxVal.toString().replace("0x", "").replace("x", ""), 16);
					else {
						try {
							val = Long.parseLong(intl.MaxVal.toString());
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
						theSetOfAttributes = theSetOfAttributes + c1.AttributeId + ",";
					}
					theSetOfAttributes = theSetOfAttributes.substring(0, theSetOfAttributes.length() - 1);

					minVal = ((Double) Double.parseDouble(intl.MinVal.toString())).toString();
					maxVal = ((Double) Double.parseDouble(intl.MaxVal.toString())).toString();
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
						+ "where sa.ATTR_ID = " + c.AttributeId + " and (MIN_VAL >= " + minVal + ") and (MAX_VAL <= "
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
				List<RowInfo> rowInfos = GetResiultsFromResultSet(rs, n);

				rs.close();

				// write the interval
				// read the result (not all the results, only top n)

				for (RowInfo ri : rowInfos) {
					Long seq = ri.Seq;
					if (!res.containsKey(seq)) {
						String statement = ri.Statement;
						Long userSession = ri.UserSession;
						try {

							accessArea = extraction.extractAccessArea(statement);

							String from = "";
							String where = "";
							from = accessArea.getFrom().toString();
							from = from.substring(1, from.length() - 1);
							where = accessArea.getWhere().toString();
							Query q = new Query(0, from, where, seq, opt, userSession);
							q.Statement = statement;
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

	private static Map<Long, Query> GetQueriesWithSimilarAttributesNumber(Long statId, Long lastUs, Table t, Column c,
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
						theSetOfAttributes = theSetOfAttributes + c1.AttributeId + ",";
					}
					theSetOfAttributes = theSetOfAttributes.substring(0, theSetOfAttributes.length() - 1);
				}
				if (attrIdAnddataType.getSecond() == 1) {
					attrIdsSize = columnsByType.get(1).size();
					for (Column c1 : columnsByType.get(1)) {
						theSetOfAttributes = theSetOfAttributes + c1.AttributeId + ",";
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
							+ " sa " + "where sa.ATTR_ID = " + c.AttributeId + " ) sa on sa.stat_id = s.seq "
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
					rowInfos = GetResiultsFromResultSet(rs, n);
				}
				rs.close();

				// write the interval
				// read the result (not all the results, only top n)

				for (RowInfo ri : rowInfos) {
					Long seq = ri.Seq;
					if (!res.containsKey(seq)) {
						String statement = ri.Statement;
						Long userSession = ri.UserSession;
						try {

							accessArea = extraction.extractAccessArea(statement);

							String from = "";
							String where = "";
							from = accessArea.getFrom().toString();
							from = from.substring(1, from.length() - 1);
							where = accessArea.getWhere().toString();
							Query q = new Query(0, from, where, seq, opt, userSession);
							q.Statement = statement;
							q.HasNotOverlap = true;
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

	private static Map<Long, Query> GetQueriesWithSimilarAttributesString(Long statId, Long lastUs, Table t, Column c,
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
					theSetOfAttributes = theSetOfAttributes + c1.AttributeId + ",";
				}
				theSetOfAttributes = theSetOfAttributes.substring(0, theSetOfAttributes.length() - 1);

				String query = "select min(s.seq) as SEQ, s.STATEMENT, sa.min_val, sa.max_val, us.USERSESSION  "
						+ "from QRS_STATEMENTS_PP s inner join QRS_USER_SESSIONS_PP us on us.seq = s.seq and us.thetime = s.thetime "
						+ "inner join (select sa.stat_id as stat_id, sa.min_val, sa.max_val from " + tableName + " sa "
						+ "where sa.ATTR_ID = " + c.AttributeId + " ) sa on sa.stat_id = s.seq "
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
				List<RowInfo> rowInfos = GetResiultsFromResultSet(rs, n);

				rs.close();

				// write the interval
				// read the result (not all the results, only top n)

				for (RowInfo ri : rowInfos) {
					Long seq = ri.Seq;
					if (!res.containsKey(seq)) {
						String statement = ri.Statement;
						Long userSession = ri.UserSession;
						try {

							accessArea = extraction.extractAccessArea(statement);

							String from = "";
							String where = "";
							from = accessArea.getFrom().toString();
							from = from.substring(1, from.length() - 1);
							where = accessArea.getWhere().toString();
							Query q = new Query(0, from, where, seq, opt, userSession);
							q.Statement = statement;
							q.HasNotOverlap = true;
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

	private static Map<Long, Query> GetQueriesWithSimilarIntervalsString(Long statId, Long lastUs, Table t, Column c,
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
					theSetOfAttributes = theSetOfAttributes + c1.AttributeId + ",";
				}
				theSetOfAttributes = theSetOfAttributes.substring(0, theSetOfAttributes.length() - 1);

				String minVal = "";
				String maxVal = "";
				// usually for string min_val and max_val are the same
				if (attrIdAnddataType.getSecond() == 3) {
					minVal = intl.MinVal.toString();
					maxVal = intl.MaxVal.toString();

				}

				String query = "select min(s.seq) as SEQ, s.STATEMENT, sa.min_val, sa.max_val, us.USERSESSION  "
						+ "from QRS_STATEMENTS_PP s inner join QRS_USER_SESSIONS_PP us on us.seq = s.seq and us.thetime = s.thetime "
						+ "inner join (select sa.stat_id as stat_id, sa.min_val, sa.max_val from " + tableName + " sa "
						+ "where sa.ATTR_ID = " + c.AttributeId + " and (MIN_VAL = " + minVal + ") and (MAX_VAL = "
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
				List<RowInfo> rowInfos = GetResiultsFromResultSet(rs, n);

				rs.close();

				// write the interval
				// read the result (not all the results, only top n)

				for (RowInfo ri : rowInfos) {
					Long seq = ri.Seq;
					if (!res.containsKey(seq)) {
						String statement = ri.Statement;
						Long userSession = ri.UserSession;
						try {

							accessArea = extraction.extractAccessArea(statement);

							String from = "";
							String where = "";
							from = accessArea.getFrom().toString();
							from = from.substring(1, from.length() - 1);
							where = accessArea.getWhere().toString();
							Query q = new Query(0, from, where, seq, opt, userSession);
							q.Statement = statement;
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

	public static List<RowInfo> GetResiultsFromResultSet(ResultSet rs, Integer n) {
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

	public static Map<Long, Query> GetQueriesWithSimilarIntervals(Long statId, Long lastUs, Table t, Column c,
			List<Interval> intervalsInQuery1, Connection conn, Pair<Integer, Integer> attrIdAnddataType, Options opt,
			Map<Long, Query> res, BufferedWriter writer, HashMap<Integer, List<Column>> columnByType) {
		// public final static Integer typeLong = 1;
		// public final static Integer typeFloat = 2;
		// public final static Integer typeString = 3;
		// public final static Integer typeDatetime = 4;

		switch (attrIdAnddataType.getSecond()) {
		case 1:
			res = GetQueriesWithSimilarIntervalsNumber(statId, lastUs, t, c, intervalsInQuery1, conn, attrIdAnddataType,
					res, "QRS_STAT_ATTR_NUMBER", opt, writer, columnByType);
			break;
		case 2:
			res = GetQueriesWithSimilarIntervalsNumber(statId, lastUs, t, c, intervalsInQuery1, conn, attrIdAnddataType,
					res, "QRS_STAT_ATTR_FLOAT", opt, writer, columnByType);
			break;
		case 3:
			res = GetQueriesWithSimilarIntervalsString(statId, lastUs, t, c, intervalsInQuery1, conn, attrIdAnddataType,
					res, "QRS_STAT_ATTR_STRING", opt, writer, columnByType);
			break;
		case 4:
			WriteDateInterval(statId, t, c, intervalsInQuery1, conn, attrIdAnddataType);
			break;
		}

		return res;
	}

	public static Map<Long, Query> GetQueriesWithSimilarAttributes(Long statId, Long lastUs, Table t, Column c,
			List<Interval> intervalsInQuery1, Connection conn, Pair<Integer, Integer> attrIdAnddataType, Options opt,
			Map<Long, Query> res, BufferedWriter writer, HashMap<Integer, List<Column>> columnsByType) {
		// public final static Integer typeLong = 1;
		// public final static Integer typeFloat = 2;
		// public final static Integer typeString = 3;
		// public final static Integer typeDatetime = 4;

		switch (attrIdAnddataType.getSecond()) {
		case 1:
			res = GetQueriesWithSimilarAttributesNumber(statId, lastUs, t, c, intervalsInQuery1, conn,
					attrIdAnddataType, res, "QRS_STAT_ATTR_NUMBER", opt, writer, columnsByType);
			break;
		case 2:
			res = GetQueriesWithSimilarAttributesNumber(statId, lastUs, t, c, intervalsInQuery1, conn,
					attrIdAnddataType, res, "QRS_STAT_ATTR_FLOAT", opt, writer, columnsByType);
			break;
		case 3:
			res = GetQueriesWithSimilarAttributesString(statId, lastUs, t, c, intervalsInQuery1, conn,
					attrIdAnddataType, res, "QRS_STAT_ATTR_STRING", opt, writer, columnsByType);
			break;
		case 4:
			WriteDateInterval(statId, t, c, intervalsInQuery1, conn, attrIdAnddataType);
			break;
		}

		return res;
	}

	private static Map<Long, String> GetNextQuery(Long userSession, Long seq, Connection conn, BufferedWriter writer) {
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

			// writer.write(query + "\n");
			rs = st.executeQuery(query);
			// write the interval
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

	public static Map<Long, Query> GetNextQueriesForUserSessions(Map<Long, Pair<Query, Double>> theNearestQueries,
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
					nextQ = GetNextQuery(theQueryWithDist.getFirst().UserSession, sq, conn, writer);
				} catch (Exception ex) {
					System.out.println(ex);
				}
				for (Long v : nextQ.keySet()) {

					if (theQueryWithDist.getFirst().HasNotOverlap == true) {
						// it doesn't matter what filtering condition the query
						// has
						String statement = nextQ.get(v);
						Query q = CreateQuery(statement, v, opt, theQueryWithDist.getFirst().UserSession);
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
							Query q = CreateQuery(statement, v, opt, theQueryWithDist.getFirst().UserSession);
							if (q != null) {
								Boolean doNotAdd = false;
								for (Query cq : res.values()) {
									if (cq.Statement.equals(q.Statement)) {
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
					q.Statement = statement;
				} catch (Exception ex) {

				}
			}
			return q;
		} catch (Exception ex) {
			return null;
		}
	}

	public static Long GetEstimetedRowCountInParentTable(Column column, Table t) {
		Long res = t.Count;
		if (column.GlobalColumnType == GlobalColumnType.DistributedFieldWithEmissions) {
			for (ValueState vs : ((DistributedFieldWithEmissions) column.Distribution).Values.values()) {
				res = vs.ValuesCount;
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
		if (t.Count > t1.Count)
			res = true;
		return res;
	}

	public static long GetEstimetedRowCountInJoin(Object val, Column column, Query q, Options opt, Long estTCount) {
		Long res = new Long(0);
		String[] vals = val.toString().split("\\.");
		String tableName = vals[0];
		String columnName = vals[1];
		Table t = opt.TABLESWITHCOUNT.get(tableName);
		res = t.Count;
		Column c = opt.COLUMNS_DISTRIBUTION.get(tableName + "." + columnName);
		if (c == null) {
			// System.out.println("column = " + columnName + "is null");
			return 0;
		} else {
			Map<String, List<Interval>> mapColIntervals1 = GetIntervalForQureryAndColumn(q, t, opt);
			if (mapColIntervals1 == null)
				return -1;
			Set<String> columnsInQuery1_ = mapColIntervals1.keySet();

			ArrayList<String> columnsInQuery1 = new ArrayList<String>();
			columnsInQuery1.addAll(columnsInQuery1_);

			Map<String, List<Interval>> mapColIntervalsAll = new TreeMap<String, List<Interval>>();
			for (String columnstr : columnsInQuery1) {
				List<Interval> intervals = mapColIntervals1.get(columnstr);
				Column c1 = opt.COLUMNS_DISTRIBUTION.get(columnstr);
				Long tmp = GetEstimatedRowsCount(intervals, c1, t);
				if (tmp < res)
					res = tmp;
			}

		}
		Double k = ((Long) t.Count).doubleValue() / estTCount;
		return Math.round(res * k);
	}

	public static Interval GetTntervalForLe(Object val, Column column) {
		Interval interval = new Interval();
		GlobalColumnType columnType = column.GlobalColumnType;
		switch (columnType) {
		case DictionaryField: {
			interval.MinVal = ((ValueState) ((DictionaryField) column.Distribution).Values.values().toArray()[0]).Value;
			interval.MaxVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = true;
		}
			break;
		case DistributedField: {
			interval.MinVal = ((DistributedField) column.Distribution).MinValue;
			interval.MaxVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = true;
		}
			break;
		case DistributedFieldWithEmissions: {
			interval.MinVal = ((DistributedFieldWithEmissions) column.Distribution).MinValue;
			for (ValueState emission : ((DistributedFieldWithEmissions) column.Distribution).Values.values()) {
				Double emissionVal = Double.parseDouble(emission.Value.toString());
				if ((Double) interval.MinVal > emissionVal)
					interval.MinVal = emissionVal;
			}
			interval.MaxVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = true;
		}
			break;
		case Identificator: {
			interval.MinVal = ((Identificator) column.Distribution).MinValue;
			interval.MaxVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = true;
		}
			break;
		case NonNumericidentifier: {
			interval.MaxVal = 1;
			interval.MinVal = 1;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = true;
		}
			break;
		}
		return interval;
	}

	public static Interval GetTntervalForLt(Object val, Column column) {
		Interval interval = new Interval();
		GlobalColumnType columnType = column.GlobalColumnType;
		switch (columnType) {
		case DictionaryField: {
			interval.MinVal = ((ValueState) ((DictionaryField) column.Distribution).Values.values().toArray()[0]).Value;
			interval.MaxVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = false;
		}
			break;
		case DistributedField: {
			interval.MinVal = ((DistributedField) column.Distribution).MinValue;
			interval.MaxVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = false;
		}
			break;
		case DistributedFieldWithEmissions: {
			interval.MinVal = ((DistributedFieldWithEmissions) column.Distribution).MinValue;
			for (ValueState emission : ((DistributedFieldWithEmissions) column.Distribution).Values.values()) {
				Double emissionVal = Double.parseDouble(emission.Value.toString());
				if ((Double) interval.MinVal > emissionVal)
					interval.MinVal = emissionVal;
			}
			interval.MaxVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = false;
		}
			break;
		case Identificator: {
			interval.MinVal = ((Identificator) column.Distribution).MinValue;
			interval.MaxVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = false;
		}
			break;
		}
		return interval;
	}

	public static Interval GetTntervalForGt(Object val, Column column) {
		Interval interval = new Interval();
		GlobalColumnType columnType = column.GlobalColumnType;
		switch (columnType) {
		case DictionaryField: {
			int valuesCount = ((DictionaryField) column.Distribution).Values.values().size();
			interval.MaxVal = ((ValueState) ((DictionaryField) column.Distribution).Values.values()
					.toArray()[valuesCount - 1]).Value;
			interval.MinVal = val;
			interval.StricktMinBorder = false;
			interval.StricktMaxBorder = true;
		}
			break;
		case DistributedField: {
			interval.MaxVal = ((DistributedField) column.Distribution).MaxValue;
			interval.MinVal = val;
			interval.StricktMinBorder = false;
			interval.StricktMaxBorder = true;
		}
			break;
		case DistributedFieldWithEmissions: {
			interval.MaxVal = ((DistributedFieldWithEmissions) column.Distribution).MaxValue;
			for (ValueState emission : ((DistributedFieldWithEmissions) column.Distribution).Values.values()) {
				Double emissionVal = Double.parseDouble(emission.Value.toString());
				if ((Double) interval.MaxVal < emissionVal)
					interval.MaxVal = emissionVal;
			}
			interval.MinVal = val;
			interval.StricktMinBorder = false;
			interval.StricktMaxBorder = true;
		}
			break;
		case Identificator: {
			interval.MaxVal = ((Identificator) column.Distribution).MaxValue;
			interval.MinVal = val;
			interval.StricktMinBorder = false;
			interval.StricktMaxBorder = true;
		}
			break;
		}
		return interval;
	}

	public static Interval GetTntervalForGe(Object val, Column column) {
		Interval interval = new Interval();
		GlobalColumnType columnType = column.GlobalColumnType;
		switch (columnType) {
		case DictionaryField: {
			int valuesCount = ((DictionaryField) column.Distribution).Values.values().size();
			interval.MaxVal = ((ValueState) ((DictionaryField) column.Distribution).Values.values()
					.toArray()[valuesCount - 1]).Value;
			interval.MinVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = true;
		}
			break;
		case DistributedField: {
			interval.MaxVal = ((DistributedField) column.Distribution).MaxValue;
			interval.MinVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = true;
		}
			break;
		case DistributedFieldWithEmissions: {
			interval.MaxVal = ((DistributedFieldWithEmissions) column.Distribution).MaxValue;
			for (ValueState emission : ((DistributedFieldWithEmissions) column.Distribution).Values.values()) {
				Double emissionVal = Double.parseDouble(emission.Value.toString());
				if ((Double) interval.MaxVal < emissionVal)
					interval.MaxVal = emissionVal;
			}
			interval.MinVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = true;
		}
			break;
		case Identificator: {
			interval.MaxVal = ((Identificator) column.Distribution).MaxValue;
			interval.MinVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = true;
		}
			break;
		case NonNumericidentifier: {
			interval.MaxVal = 1;
			interval.MinVal = 1;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = true;
		}
			break;
		}
		return interval;
	}

	public static List<Interval> GetTntervalForNe(Object val, Column column) {
		List<Interval> intervals = new ArrayList<Interval>();

		GlobalColumnType columnType = column.GlobalColumnType;
		Interval interval = new Interval();
		Interval interval2 = new Interval();
		switch (columnType) {
		case DictionaryField: {
			int valuesCount = ((DictionaryField) column.Distribution).Values.values().size();
			interval.MinVal = ((ValueState) ((DictionaryField) column.Distribution).Values.values().toArray()[0]).Value;
			interval.MaxVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = false;

			interval2.MinVal = val;
			interval2.MaxVal = ((ValueState) ((DictionaryField) column.Distribution).Values.values()
					.toArray()[valuesCount - 1]).Value;
			interval2.StricktMinBorder = false;
			interval2.StricktMaxBorder = true;
		}
			break;
		case DistributedField: {
			interval.MinVal = ((DistributedField) column.Distribution).MinValue;
			interval.MaxVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = false;

			interval2.MinVal = val;
			interval2.MaxVal = ((DistributedField) column.Distribution).MaxValue;
			interval2.StricktMinBorder = false;
			interval2.StricktMaxBorder = true;
		}
			break;
		case DistributedFieldWithEmissions: {
			interval.MinVal = ((DistributedFieldWithEmissions) column.Distribution).MinValue;
			for (ValueState emission : ((DistributedFieldWithEmissions) column.Distribution).Values.values()) {
				Double emissionVal = Double.parseDouble(emission.Value.toString());
				if ((Double) interval.MinVal > emissionVal)
					interval.MinVal = emissionVal;
			}
			interval.MaxVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = false;

			interval2.MaxVal = ((DistributedFieldWithEmissions) column.Distribution).MaxValue;
			for (ValueState emission : ((DistributedFieldWithEmissions) column.Distribution).Values.values()) {
				Double emissionVal = Double.parseDouble(emission.Value.toString());
				if ((Double) interval2.MaxVal < emissionVal)
					interval2.MaxVal = emission.Value;
			}
			interval2.MinVal = val;
			interval2.StricktMinBorder = false;
			interval2.StricktMaxBorder = true;
		}
			break;
		case Identificator: {
			interval.MinVal = ((Identificator) column.Distribution).MinValue;
			interval.MaxVal = val;
			interval.StricktMinBorder = true;
			interval.StricktMaxBorder = false;

			interval2.MinVal = val;
			interval2.MaxVal = ((Identificator) column.Distribution).MaxValue;
			interval2.StricktMinBorder = false;
			interval2.StricktMaxBorder = true;
		}
			break;
		}
		intervals.add(interval);
		intervals.add(interval2);
		return intervals;
	}

	public static List<Interval> GetIntervalForPredicate(Operator op, Object val, Column column, Query q, Options opt,
			Table t) {
		List<Interval> intervals = new ArrayList<Interval>();

		switch (op) {
		case EQ: {

			Interval interval = GetTntervalForEq(val, column, q, opt, t);
			if (interval != null)
				intervals.add(interval);
		}
			break;
		case LT: {
			Interval interval = GetTntervalForLt(val, column);
			intervals.add(interval);
		}
			break;
		case GT: {
			Interval interval = GetTntervalForGt(val, column);
			intervals.add(interval);
		}
			break;
		case LE: {
			Interval interval = GetTntervalForLe(val, column);
			intervals.add(interval);
		}
			break;
		case GE: {
			Interval interval = GetTntervalForGe(val, column);
			intervals.add(interval);
		}
			break;
		case NE: {
			intervals = GetTntervalForNe(val, column);
		}
			break;
		}
		return intervals;
	}

	public static List<Interval> MergeIntervalsByDisjunctionForDictionaryField(List<Interval> originalIntervals,
			Column column) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		SortedMap<String, Point> points = new TreeMap<String, Point>();
		for (Interval interval : originalIntervals) {
			if (!interval.HasOnlyIntervalsEstimatedRowCount) {
				try {
					Point p = new Point();
					p.Value = interval.MinVal;
					p.IsMinBorder = true;
					p.IsStrict = interval.StricktMinBorder;
					points.put((String) interval.MinVal, p);

					Point p2 = new Point();
					p2.Value = (String) interval.MaxVal;
					p2.IsMinBorder = false;
					p2.IsStrict = interval.StricktMaxBorder;
					if (points.containsKey((String) interval.MaxVal)) {
						resIntervals.add(interval);
					} else {
						points.put((String) interval.MaxVal, p2);
					}
				} catch (Exception e) {
					String exstr = e.toString();
				}
			} else
				resIntervals.add(interval);
		}

		List<Interval> r = (GetFinalIntervalsForDisjunction(points));
		for (Interval intl : r) {
			resIntervals.add(intl);
		}
		return resIntervals;
	}

	public static List<Interval> MergeIntervalsByDisjunctionForDistributedField(List<Interval> originalIntervals,
			Column column) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		SortedMap<Double, Point> points = new TreeMap<Double, Point>();
		for (Interval interval : originalIntervals) {
			if (!interval.HasOnlyIntervalsEstimatedRowCount) {
				try {
					Point p = new Point();
					Double val = Double.parseDouble(interval.MinVal.toString().replace("'", ""));
					p.Value = val;
					p.IsMinBorder = true;
					p.IsStrict = interval.StricktMinBorder;
					points.put(val, p);

					Point p2 = new Point();
					val = Double.parseDouble(interval.MaxVal.toString().replace("'", ""));
					p2.Value = val;
					p2.IsMinBorder = false;
					p2.IsStrict = interval.StricktMaxBorder;
					points.put(val, p2);
				} catch (Exception ex) {
					///// System.out.println("column.Name = " + column.Name + ";
					///// ex = " + ex);
					return null;
				}
			} else
				resIntervals.add(interval);
		}

		List<Interval> r = (GetFinalIntervalsForDisjunction(points));
		for (Interval intl : r) {
			resIntervals.add(intl);
		}
		return resIntervals;
	}

	public static List<Interval> MergeIntervalsByDisjunctionForIdentificator(List<Interval> originalIntervals,
			Column column) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		SortedMap<Long, Point> points = new TreeMap<Long, Point>();
		for (Interval interval : originalIntervals) {
			if (!interval.HasOnlyIntervalsEstimatedRowCount) {
				Point p = new Point();

				Long val = new Long(0);
				if (interval.MinVal.toString().contains("x"))
					val = Long.parseLong(interval.MinVal.toString().replace("0x", "").replace("x", ""), 16);
				else {
					try {
						val = Long.parseLong(interval.MinVal.toString());
					} catch (Exception ex) {
						///// System.out.println("column.Name = " + column.Name
						///// + "; ex = " + ex);
						return null;
					}
				}
				p.Value = val;
				p.IsMinBorder = true;
				p.IsStrict = interval.StricktMinBorder;
				points.put(val, p);

				Point p2 = new Point();
				if (interval.MaxVal.toString().contains("x"))
					val = Long.parseLong(interval.MaxVal.toString().replace("0x", "").replace("x", ""), 16);
				else {
					try {
						val = Long.parseLong(interval.MaxVal.toString());
					} catch (Exception ex) {
						// System.out.println("column.Name = " + column.Name +
						// "; ex = " + ex);
						return null;
					}
				}
				p2.Value = val;
				p2.IsMinBorder = false;
				p2.IsStrict = interval.StricktMaxBorder;
				if (points.containsKey(val))
					resIntervals.add(interval);
				else
					points.put(val, p2);
			} else
				resIntervals.add(interval);
		}

		List<Interval> r = (GetFinalIntervalsForDisjunction(points));
		for (Interval intl : r) {
			resIntervals.add(intl);
		}
		return resIntervals;
	}

	public static <T> List<Interval> GetFinalIntervalsForDisjunction(SortedMap<T, Point> points) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		Boolean isStart = true;
		Interval intl = new Interval();
		Integer iInt = 0;
		// now we have sorted points
		for (Point p : points.values()) {
			if (isStart) {
				intl = new Interval();
				intl.MinVal = p.Value;
				intl.StricktMinBorder = p.IsStrict;
				// iInt++;
			}
			isStart = false;

			if (p.IsMinBorder)
				iInt++;
			else
				iInt--;

			if (iInt <= 0) {
				intl.MaxVal = p.Value;
				intl.StricktMaxBorder = p.IsStrict;

				Interval intl2 = new Interval();
				intl2.MaxVal = intl.MaxVal;
				intl2.MinVal = intl.MinVal;
				intl2.StricktMaxBorder = intl.StricktMaxBorder;
				intl2.StricktMinBorder = intl.StricktMinBorder;
				resIntervals.add(intl2);
				isStart = true;
			}

		}
		return resIntervals;
	}

	public static List<Interval> MergeIntervalsByDisjunction(List<Interval> originalIntervals, Column column) {
		Object points = null;
		List<Interval> resIntervals = new ArrayList<Interval>();
		points = new TreeMap<String, Point>();
		switch (column.GlobalColumnType) {
		case DictionaryField: {
			resIntervals = MergeIntervalsByDisjunctionForDictionaryField(originalIntervals, column);
		}
			break;
		case DistributedField: {
			resIntervals = MergeIntervalsByDisjunctionForDistributedField(originalIntervals, column);
		}
			break;
		case DistributedFieldWithEmissions: {
			resIntervals = MergeIntervalsByDisjunctionForDistributedField(originalIntervals, column);
		}
			break;
		case Identificator: {
			resIntervals = MergeIntervalsByDisjunctionForIdentificator(originalIntervals, column);
		}
			break;
		}
		return resIntervals;
	}

	public static List<Interval> MergeIntervalsByConjunction(List<List<Interval>> originalIntervals, Column column) {
		Object points = null;
		List<Interval> resIntervals = new ArrayList<Interval>();
		points = new TreeMap<String, Point>();
		switch (column.GlobalColumnType) {
		case DictionaryField: {
			resIntervals = MergeIntervalsByConjunctionForDictionaryField(originalIntervals, column);
		}
			break;
		case DistributedField: {
			resIntervals = MergeIntervalsByConjunctionForDistributedField(originalIntervals, column);
		}
			break;
		case DistributedFieldWithEmissions: {
			resIntervals = MergeIntervalsByConjunctionForDistributedField(originalIntervals, column);
		}
			break;
		case Identificator: {
			resIntervals = MergeIntervalsByConjunctionForIdentificator(originalIntervals, column);
		}
			break;
		}
		return resIntervals;
	}

	public static <T> List<Interval> GetFinalIntervalsForConjunction(List<List<Interval>> originalIntervals,
			SortedMap<T, List<Point>> points) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		Interval intl = new Interval();
		int needHasCount = originalIntervals.size();
		List<String> has = new ArrayList<String>();
		// now we have sorted points
		for (List<Point> point : points.values()) {
			for (Point p : point) {

				if (p.IsMinBorder) {
					has.add(p.Index.toString());
					if (intl.MaxVal != null) {
						if (intl.MaxVal.equals(p.Value))
							has.add(intl.Index);
					}
					intl = new Interval();
					intl.MinVal = p.Value;
					intl.Index = p.Index.toString();
					intl.StricktMinBorder = p.IsStrict;
				} else {
					intl.MaxVal = p.Value;
					intl.StricktMaxBorder = p.IsStrict;
					if (has.size() == needHasCount) {

						Interval intl2 = new Interval();
						intl2.MaxVal = intl.MaxVal;
						intl2.MinVal = intl.MinVal;
						intl2.StricktMaxBorder = intl.StricktMaxBorder;
						intl2.StricktMinBorder = intl.StricktMinBorder;
						resIntervals.add(intl2);

					}
					has.remove(p.Index.toString());
				}
			}
		}
		return resIntervals;
	}

	public static List<Interval> MergeIntervalsByConjunctionForIdentificator(List<List<Interval>> originalIntervals,
			Column column) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		SortedMap<Long, List<Point>> points = new TreeMap<Long, List<Point>>();

		int i = 0;
		for (List<Interval> listIntervals : originalIntervals) {
			for (Interval interval : listIntervals) {
				if (!interval.HasOnlyIntervalsEstimatedRowCount) {
					Point p = new Point();

					Long minVal = new Long(0);
					Long maxVal = new Long(0);

					if (interval.MinVal.toString().contains("x")) {
						minVal = Long.parseLong(interval.MinVal.toString().replace("0x", "").replace("x", ""), 16);
						maxVal = Long.parseLong(interval.MaxVal.toString().replace("0x", "").replace("x", ""), 16);
					} else {
						minVal = Long.parseLong(interval.MinVal.toString());
						maxVal = Long.parseLong(interval.MaxVal.toString());
					}

					p.Value = minVal;
					p.IsMinBorder = true;
					p.IsStrict = interval.StricktMinBorder;
					p.Index = i;
					if (!points.containsKey(minVal)) {
						List<Point> point = new ArrayList<Point>();
						point.add(p);
						points.put((Long) minVal, point);
					} else
						points.get((Long) minVal).add(p);

					Point p2 = new Point();
					p2.Value = (Long) maxVal;
					p2.IsMinBorder = false;
					p2.IsStrict = interval.StricktMaxBorder;
					p2.Index = i;
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

		List<Interval> r = GetFinalIntervalsForConjunction(originalIntervals, points);
		for (Interval intl : r) {
			resIntervals.add(intl);
		}
		return resIntervals;
	}

	public static List<Interval> MergeIntervalsByConjunctionForDistributedField(List<List<Interval>> originalIntervals,
			Column column) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		SortedMap<Double, List<Point>> points = new TreeMap<Double, List<Point>>();

		int i = 0;
		for (List<Interval> listIntervals : originalIntervals) {
			for (Interval interval : listIntervals) {
				if (!interval.HasOnlyIntervalsEstimatedRowCount) {
					Point p = new Point();
					p.Value = (Double) interval.MinVal;
					p.IsMinBorder = true;
					p.IsStrict = interval.StricktMinBorder;
					p.Index = i;
					if (!points.containsKey(interval.MinVal)) {
						List<Point> point = new ArrayList<Point>();
						point.add(p);
						points.put((Double) interval.MinVal, point);
					} else
						points.get((Double) interval.MinVal).add(p);

					Point p2 = new Point();
					p2.Value = (Double) interval.MaxVal;
					p2.IsMinBorder = false;
					p2.IsStrict = interval.StricktMaxBorder;
					p2.Index = i;
					if (!points.containsKey(interval.MaxVal)) {
						List<Point> point = new ArrayList<Point>();
						point.add(p2);
						points.put((Double) interval.MaxVal, point);
					} else
						points.get((Double) interval.MaxVal).add(p2);
				}

			}
			i++;
		}

		List<Interval> r = GetFinalIntervalsForConjunction(originalIntervals, points);
		for (Interval intl : r) {
			resIntervals.add(intl);
		}
		return resIntervals;
	}

	public static List<Interval> MergeIntervalsByConjunctionForDictionaryField(List<List<Interval>> originalIntervals,
			Column column) {
		List<Interval> resIntervals = new ArrayList<Interval>();
		SortedMap<String, List<Point>> points = new TreeMap<String, List<Point>>();

		int i = 0;
		for (List<Interval> listIntervals : originalIntervals) {
			for (Interval interval : listIntervals) {
				if (!interval.HasOnlyIntervalsEstimatedRowCount) {
					Point p = new Point();
					p.Value = (String) interval.MinVal;
					p.IsMinBorder = true;
					p.IsStrict = interval.StricktMinBorder;
					p.Index = i;
					if (!points.containsKey(interval.MinVal)) {
						List<Point> point = new ArrayList<Point>();
						point.add(p);
						points.put((String) interval.MinVal, point);
					} else
						points.get((String) interval.MinVal).add(p);

					Point p2 = new Point();
					p2.Value = (String) interval.MaxVal;
					p2.IsMinBorder = false;
					p2.IsStrict = interval.StricktMaxBorder;
					p2.Index = i;
					if (!points.containsKey(interval.MaxVal)) {
						List<Point> point = new ArrayList<Point>();
						point.add(p2);
						points.put((String) interval.MaxVal, point);
					} else
						points.get((String) interval.MaxVal).add(p2);
				}

			}
			i++;
		}

		List<Interval> r = GetFinalIntervalsForConjunction(originalIntervals, points);
		for (Interval intl : r) {
			resIntervals.add(intl);
		}
		return resIntervals;
	}

	public static void WriteIntervalForQureryAndColumn(Long statId, Query q1, Table t, Options opt, Connection conn) {
		Map<String, List<Interval>> res = new TreeMap<String, List<Interval>>();
		// Map<String, Map<Integer, List<Predicate>>> columnsListpred1 =
		// GetPredicateForEachColumn(q1, t, opt);
		Map<String, List<Interval>> mapColIntervals1 = GetIntervalForQureryAndColumn(q1, t, opt);

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
				attributeBitmap = SetAttributeMask(attributeBitmap, c.AttributeId);

				String columnName = c.Name.toLowerCase();
				Pair<Integer, Integer> attrIdAnddataType = t.Columns.get(columnName);
				WriteIntervalsToDB(statId, t, c, intervals, conn, attrIdAnddataType);
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
			WriteAttributeBitmask(attributeBitmap, conn);
		return;// res;
	}

	public static Long[] SetAttributeMask(Long[] attrMask, Integer attributeId) {
		int group = attributeId / 63;
		int indexInGroup = attributeId % 63;
		attrMask[group] |= Math.round(Math.pow(2, indexInGroup));
		return attrMask;
	}

	public static void WriteIntervalsToDB(Long statId, Table t, Column c, List<Interval> intervalsInQuery1,
			Connection conn, Pair<Integer, Integer> attrIdAnddataType) {
		// public final static Integer typeLong = 1;
		// public final static Integer typeFloat = 2;
		// public final static Integer typeString = 3;
		// public final static Integer typeDatetime = 4;

		switch (attrIdAnddataType.getSecond()) {
		case 1:
			WriteLongInterval(statId, t, c, intervalsInQuery1, conn, attrIdAnddataType);
			break;
		case 2:
			WriteFloatInterval(statId, t, c, intervalsInQuery1, conn, attrIdAnddataType);
			break;
		case 3:
			WriteStringInterval(statId, t, c, intervalsInQuery1, conn, attrIdAnddataType);
			break;
		case 4:
			WriteDateInterval(statId, t, c, intervalsInQuery1, conn, attrIdAnddataType);
			break;
		}

	}

	private static void WriteDateInterval(Long statId, Table t, Column c, List<Interval> intervalsInQuery1,
			Connection conn, Pair<Integer, Integer> attrIdAnddataType) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			ResultSet rs = null;

			for (Interval intl : intervalsInQuery1) {

				String query = "MERGE INTO QRS_STAT_ATTR_DATETIME t " + "USING (SELECT " + statId + " as STAT_ID, "
						+ c.AttributeId + " as ATTR_ID  from dual) h "
						+ "ON (h.STAT_ID = t.STAT_ID and h.ATTR_ID = t.ATTR_ID) " + "WHEN NOT MATCHED THEN "
						+ "INSERT (STAT_ID,	ATTR_ID, MIN_VAL, MAX_VAL) VALUES (" + statId + "," + c.AttributeId + ", "
						+ (String) intl.MinVal + ", " + (String) intl.MaxVal + ")";

				rs = st.executeQuery(query);
				// write the interval

			}
		} catch (Exception ex) {

		}
	}

	private static void WriteStringInterval(Long statId, Table t, Column c, List<Interval> intervalsInQuery1,
			Connection conn, Pair<Integer, Integer> attrIdAnddataType) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			ResultSet rs = null;

			for (Interval intl : intervalsInQuery1) {
				String query = "MERGE INTO QRS_STAT_ATTR_STRING t " + "USING (SELECT " + statId + " as STAT_ID, "
						+ c.AttributeId + " as ATTR_ID from dual) h "
						+ "ON (h.STAT_ID = t.STAT_ID and h.ATTR_ID = t.ATTR_ID) " + "WHEN NOT MATCHED THEN "
						+ "INSERT (STAT_ID,	ATTR_ID, MIN_VAL, MAX_VAL) VALUES (" + statId + "," + c.AttributeId + ", "
						+ (String) intl.MinVal + ", " + (String) intl.MaxVal + ")";

				rs = st.executeQuery(query);
				// write the interval

			}
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}

	private static void WriteAttributeBitmask(Long[] attributeMask, Connection conn) {
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
			// write the interval

		} catch (Exception ex) {
			System.out.println(ex);
		}
	}

	private static void WriteFloatInterval(Long statId, Table t, Column c, List<Interval> intervalsInQuery1,
			Connection conn, Pair<Integer, Integer> attrIdAnddataType) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			ResultSet rs = null;

			for (Interval intl : intervalsInQuery1) {
				String query = "MERGE INTO QRS_STAT_ATTR_FLOAT t " + "USING (SELECT " + statId + " as STAT_ID, "
						+ c.AttributeId + " as ATTR_ID from dual) h "
						+ "ON (h.STAT_ID = t.STAT_ID and h.ATTR_ID = t.ATTR_ID) " + "WHEN NOT MATCHED THEN "
						+ "INSERT (STAT_ID,	ATTR_ID, MIN_VAL, MAX_VAL) VALUES (" + statId + "," + c.AttributeId + ", "
						+ Double.parseDouble(intl.MinVal.toString()) + ", " + Double.parseDouble(intl.MaxVal.toString())
						+ ")";

				rs = st.executeQuery(query);
				// write the interval

			}
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}

	private static void WriteLongInterval(Long statId, Table t, Column c, List<Interval> intervalsInQuery1,
			Connection conn, Pair<Integer, Integer> attrIdAnddataType) {
		try {
			// TODO Auto-generated method stub
			Statement st = conn.createStatement();
			ResultSet rs = null;

			for (Interval intl : intervalsInQuery1) {
				Long minVal = new Long(0);
				Long maxVal = new Long(0);
				if (intl.MinVal.toString().contains(".")) {
					minVal = (long) Double.parseDouble(intl.MinVal.toString());
					maxVal = (long) Double.parseDouble(intl.MaxVal.toString());
				} else {
					if (intl.MinVal.toString().contains("x")) {
						minVal = Long.parseLong(intl.MinVal.toString().replace("0x", "").replace("x", ""), 16);
						maxVal = Long.parseLong(intl.MaxVal.toString().replace("0x", "").replace("x", ""), 16);
					} else {
						minVal = Long.parseLong(intl.MinVal.toString());
						maxVal = Long.parseLong(intl.MaxVal.toString());
					}
				}
				String query = "MERGE INTO QRS_STAT_ATTR_NUMBER t " + "USING (SELECT " + statId + " as STAT_ID, "
						+ c.AttributeId + " as ATTR_ID  from dual) h "
						+ "ON (h.STAT_ID = t.STAT_ID and h.ATTR_ID = t.ATTR_ID) " + "WHEN NOT MATCHED THEN "
						+ "INSERT (STAT_ID,	ATTR_ID, MIN_VAL, MAX_VAL) VALUES (" + statId + "," + c.AttributeId + ", "
						+ minVal + ", " + maxVal + ")";

				rs = st.executeQuery(query);
				// write the interval

			}
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}

	public static Map<String, List<Interval>> GetIntervalForQureryAndColumn(Query q1, Table t, Options opt) {
		Map<String, List<Interval>> res = new TreeMap<String, List<Interval>>();
		Map<String, Map<Integer, List<Predicate>>> columnsListpred1 = GetPredicateForEachColumn(q1, t, opt);

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
						List<Interval> tmp = GetIntervalForPredicate(op, val, c, q1, opt, t);
						for (Interval intl : tmp) {
							intervals.add(intl);
						}
					}
				}
				if (c != null) {

					List<Interval> intervals2 = MergeIntervalsByDisjunction(intervals, c);
					if (intervals2 == null)
						return null;
					intervalsAll.add(intervals2);
				}

			}
			if (c != null) {
				List<Interval> intervalsInQuery1 = MergeIntervalsByConjunction(intervalsAll, c);

				res.put(c.Name, intervalsInQuery1);
			}
		}
		return res;
	}

	public static long GetEstimatedRowsOverall(Query q1, Query q2, Table t, Options opt) {
		if (t == null)
			return 0;
		long res1 = t.Count;
		long res2 = t.Count;

		Map<String, List<Interval>> mapColIntervals1 = GetIntervalForQureryAndColumn(q1, t, opt);
		Map<String, List<Interval>> mapColIntervals2 = GetIntervalForQureryAndColumn(q2, t, opt);

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
				tmp = GetEstimatedRowsCount(intervals, c, t);
			if (tmp < res1)
				res1 = tmp;
		}

		for (String column : columnsInQuery2) {
			List<Interval> intervals = mapColIntervals2.get(column);
			Column c = opt.COLUMNS_DISTRIBUTION.get(column);
			Long tmp = new Long(0);
			if (intervals.size() != 0)
				tmp = GetEstimatedRowsCount(intervals, c, t);
			if (tmp < res2)
				res2 = tmp;
		}

		return res1 + res2;
	}

	public static Long GetEstimatedRowsOverlap(Query q1, Query q2, Table t, Options opt) {
		if (t == null)
			return new Long(0);
		Long res = t.Count;
		Double result = new Double(0);
		Map<String, List<Interval>> mapColIntervals1 = GetIntervalForQureryAndColumn(q1, t, opt);
		Map<String, List<Interval>> mapColIntervals2 = GetIntervalForQureryAndColumn(q2, t, opt);

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
				List<Interval> intervalsall = MergeIntervalsByConjunction(intervalAll, c);
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
				tmp = GetEstimatedRowsCount(intervals, c, t);
			if (tmp < res)
				res = tmp;
		}
		result = res.doubleValue() / t.Count;

		Long res1 = t.Count;
		Set<String> columnsInQueryOnlyIn1_ = mapColIntervalsOnlyIn1.keySet();

		ArrayList<String> columnsInQueryOnlyIn1 = new ArrayList<String>();
		columnsInQueryOnlyIn1.addAll(columnsInQueryOnlyIn1_);
		for (String column : columnsInQueryOnlyIn1) {
			List<Interval> intervals = mapColIntervalsOnlyIn1.get(column);
			Column c = opt.COLUMNS_DISTRIBUTION.get(column);
			Long tmp = new Long(0);
			if (intervals.size() != 0)
				tmp = GetEstimatedRowsCount(intervals, c, t);
			if (tmp < res1)
				res1 = tmp;
		}

		Long res2 = t.Count;
		Set<String> columnsInQueryOnlyIn2_ = mapColIntervalsOnlyIn2.keySet();

		ArrayList<String> columnsInQueryOnlyIn2 = new ArrayList<String>();
		columnsInQueryOnlyIn2.addAll(columnsInQueryOnlyIn2_);
		for (String column : columnsInQueryOnlyIn2) {
			List<Interval> intervals = mapColIntervalsOnlyIn2.get(column);
			Column c = opt.COLUMNS_DISTRIBUTION.get(column);
			Long tmp = new Long(0);
			if (intervals.size() != 0)
				tmp = GetEstimatedRowsCount(intervals, c, t);
			if (tmp < res2)
				res2 = tmp;
		}

		Double result2 = (res1.doubleValue() * res2.doubleValue()) / (t.Count * t.Count);
		if (result2 < result)
			result = result2;
		Long finalRes = Math.round(result * t.Count);
		return finalRes;
	}

	public static Long GetEstimatedRowsCount(List<Interval> intervals, Column column, Table t) {
		Long res = new Long(0);
		Long res2 = t.Count;
		// TODO: implementation
		for (Interval interval : intervals) {
			if (!interval.HasOnlyIntervalsEstimatedRowCount)
				// calculate the estimated row count inside the interval
				res = res + GetEstimatedRowsCountInsideTheInterval(interval, column, t);
			else {
				Long r = GetEstimatedRowsCountInsideTheInterval(interval, column, t);
				if (res2 > r)
					res2 = r;
			}
		}
		if (res == 0)
			res = t.Count;
		return res < res2 ? res : res2;
	}

	public static Long GetEstimatedRowsCountInsideTheIntervalForDictionaryField(Interval interval, Column column,
			Table t) {
		Long res = new Long(0);
		String minVal = interval.MinVal.toString().replace("'", "");
		String maxVal = interval.MaxVal.toString().replace("'", "");

		Boolean stricktMinBorder = interval.StricktMinBorder;
		Boolean stricktMaxBorder = interval.StricktMaxBorder;

		DictionaryField dictionaryField = (DictionaryField) column.Distribution;
		ValueState minValueState = dictionaryField.Values.get(minVal);
		ValueState maxValueState = dictionaryField.Values.get(maxVal);

		if (minVal.equals(maxVal)) {
			if (minValueState == null) {
				res = new Long(0);
			} else
				res = minValueState.ValuesCount;
		} else {
			Long minValCount = new Long(0);
			Long maxValCount = new Long(0);
			if (stricktMinBorder)
				minValCount = minValueState.ValuesLessOrEqualCount - minValueState.ValuesCount;
			else
				minValCount = minValueState.ValuesLessOrEqualCount;

			if (stricktMaxBorder)
				maxValCount = maxValueState.ValuesLessOrEqualCount;
			else
				maxValCount = maxValueState.ValuesLessOrEqualCount - maxValueState.ValuesCount;
			res = Math.abs(maxValCount - minValCount);

		}
		return res;
	}

	public static Long GetEstimatedRowsCountInsideTheIntervalForDistributedField(Interval interval, Column column,
			Table t) {
		Long res = new Long(0);
		if (interval.HasOnlyIntervalsEstimatedRowCount)
			return interval.EstimatedRowCount;
		Double minVal = (Double) interval.MinVal;
		Double maxVal = (Double) interval.MaxVal;

		DistributedField distributedField = (DistributedField) column.Distribution;
		Double minValueState = distributedField.MinValue;
		Double maxValueState = distributedField.MaxValue;

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
			res = Math.round(t.Count * intervalWidth / columnWidth);
		}
		return res;
	}

	public static Long GetEstimatedRowsCountInsideTheIntervalForDistributedFieldWithEmissions(Interval interval,
			Column column, Table t) {
		Long res = new Long(0);
		if (interval.HasOnlyIntervalsEstimatedRowCount)
			return interval.EstimatedRowCount;
		Double minVal = (Double) interval.MinVal;
		Double maxVal = (Double) interval.MaxVal;

		Boolean stricktMinBorder = interval.StricktMinBorder;
		Boolean stricktMaxBorder = interval.StricktMaxBorder;

		DistributedFieldWithEmissions distributedFieldWithEmissions = (DistributedFieldWithEmissions) column.Distribution;
		Double minValueState = distributedFieldWithEmissions.MinValue;
		Double maxValueState = distributedFieldWithEmissions.MaxValue;

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
			res = Math.round(t.Count * intervalWidth / columnWidth);
		}

		for (ValueState emission : distributedFieldWithEmissions.Values.values()) {
			Double emissionVal = Double.parseDouble(emission.Value.toString());
			if (stricktMinBorder && stricktMaxBorder) {
				if ((emissionVal >= minVal) && (emissionVal <= maxVal))
					res = res + emission.ValuesCount;
			}
			if (stricktMinBorder && !stricktMaxBorder) {
				if ((emissionVal >= minVal) && (emissionVal < maxVal))
					res = res + emission.ValuesCount;
			}
			if (!stricktMinBorder && stricktMaxBorder) {
				if ((emissionVal > minVal) && (emissionVal <= maxVal))
					res = res + emission.ValuesCount;
			}
			if (!stricktMinBorder && !stricktMaxBorder) {
				if ((emissionVal >= minVal) && (emissionVal < maxVal))
					res = res + emission.ValuesCount;
			}
		}
		return res;
	}

	public static Long GetEstimatedRowsCountInsideTheIntervalForIdentificator(Interval interval, Column column,
			Table t) {
		Long res = new Long(0);
		if (interval.HasOnlyIntervalsEstimatedRowCount)
			return interval.EstimatedRowCount;
		Long minVal = (Long) interval.MinVal;
		Long maxVal = (Long) interval.MaxVal;

		Identificator identificatorField = (Identificator) column.Distribution;
		Long minValueState = identificatorField.MinValue;
		Long maxValueState = identificatorField.MaxValue;

		if (minVal.equals(maxVal)) {
			res = new Long(1);
		} else {
			Long intervalWidth = (Long) Math.abs(maxVal - minVal);
			if (intervalWidth == 0)
				intervalWidth = (long) 1;
			Long columnWidth = (Long) Math.abs(maxValueState - minValueState);
			if (columnWidth == 0)
				columnWidth = (long) 1;
			res = Math.round(t.Count * (double) intervalWidth / columnWidth);
		}
		return res;
	}

	public static Long GetEstimatedRowsCountInsideTheInterval(Interval interval, Column column, Table t) {
		Long res = new Long(0);

		switch (column.GlobalColumnType) {
		case DictionaryField: {
			res = GetEstimatedRowsCountInsideTheIntervalForDictionaryField(interval, column, t);
		}
			break;
		case DistributedField: {
			res = GetEstimatedRowsCountInsideTheIntervalForDistributedField(interval, column, t);
		}
			break;
		case DistributedFieldWithEmissions: {
			res = GetEstimatedRowsCountInsideTheIntervalForDistributedFieldWithEmissions(interval, column, t);
		}
			break;
		case Identificator: {
			res = GetEstimatedRowsCountInsideTheIntervalForIdentificator(interval, column, t);
		}
			break;
		case NonNumericidentifier: {
			res = new Long(1);
		}
			break;
		}
		return res;
	}

	public static Map<String, Map<Integer, List<Predicate>>> GetPredicateForEachColumn(Query q, Table t, Options opt) {
		Map<String, Map<Integer, List<Predicate>>> res = new TreeMap<String, Map<Integer, List<Predicate>>>();
		List<List<Predicate>> predicatesInThisQuery = q.whereClausesTerms;
		long minimumRowCount = t.Count;

		int iList = 0;
		for (List<Predicate> lispredicate : predicatesInThisQuery) {
			int i = 0;
			for (Predicate orPredicate : lispredicate) {
				String column = orPredicate.column;
				String table = orPredicate.table;
				String value = orPredicate.value;
				Operator operator = orPredicate.op;

				if (table.equals(t.Name)) {
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
