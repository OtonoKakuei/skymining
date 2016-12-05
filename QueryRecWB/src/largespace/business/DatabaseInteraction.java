package largespace.business;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import aima.core.util.datastructure.Pair;
import wb.model.TupleInfo;

public class DatabaseInteraction {
	private static final String QRS_PROCESSED_STRAYS = "QRS_PROCESSED_STRAYS";
	private static final String QRS_DB_SCHEMA = "QRS_DB_SCHEMA";
	private static final String QRS_QUERY_TUPLE_STRING = "QRS_QUERY_TUPLE_STRING";
	private static final String QRS_QUERY_TUPLE_NUMERIC = "QRS_QUERY_TUPLE_NUMERIC";
	private static final String QRS_PROBLEMATIC_SEQUENCES = "QRS_PROBLEMATIC_SEQUENCES";
	public static Connection conn;
	
	private static final DatabaseInteraction INSTANCE = new DatabaseInteraction();
	private static final String QRS_COMPARABLE_SEQUENCES = null;
	
	private DatabaseInteraction() {
		
	}
	
	public static void establishConnection(String serverAddress, String username, String password) {
		// Establish connection
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			// e.printStackTrace();
		}
		try {
			// startTime = new Date().getTime();
			conn = DriverManager.getConnection("jdbc:oracle:thin:@" + serverAddress, username, password);
			// conn.close();
			conn.setAutoCommit(false);
			System.out.println("Connection established");

		} catch (Exception e) {
			System.err.println("Could not establish Connection");
			// e.printStackTrace();
		}
	}

	public static void closeConnection() {
		try {
			// endTime = new Date().getTime();
			// System.out.println(endTime-startTime + " milliseconds");
			conn.close();
			System.out.println("Connection closed");

		} catch (Exception e) {
			System.err.println("Could not close Connection");
		}
	}

	public static HashMap<String, Table> getTablesKeys() {
		HashMap<String, Table> map = new HashMap<>();
		try {
			Statement st = conn.createStatement();
			ResultSet rs = null;

			rs = st.executeQuery(
					"SELECT TABLE_ID, TABLE_NAME,    COLUMN_NAME,    COLUMN_TYPE, ATTRIBUTE_ID, IS_KEY FROM QRS_DB_SCHEMA WHERE IS_KEY = 1");
			while (rs.next()) {
				Table t = new Table(rs, true);

				map.put(t.name, t);
			}
			rs.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return map;
	}

	public static List<RowInfo> getAllRelevantStatements(OptionsOwn opt) {
		List<RowInfo> res = new ArrayList<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			rs = st.executeQuery("select seq, NRROWS, statement, from_statement from " + opt.logTable + " where nrrows > 0 "
					+ "AND LOWER(statement) NOT LIKE '%create table%' AND LOWER(statement) NOT LIKE 'declare %' order by seq");
			while (rs.next()) {
				RowInfo ri = new RowInfo(rs, true);
				res.add(ri);
			}
			rs.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return res;
	}
	
	private List<RowInfo> getAllStatementsFromTable(OptionsOwn opt, String tableName) {
		List<RowInfo> res = new ArrayList<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			rs = st.executeQuery("select a.seq, NRROWS, statement, from_statement from " + opt.logTable
					+ " a join + " + tableName + " b on " + "a.seq = b.seq order by seq");
			while (rs.next()) {
				RowInfo ri = new RowInfo(rs, true);
				res.add(ri);
			}
			rs.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return res;
	}
	
	public static RowInfo getRowInfo(OptionsOwn opt, long seq) {
		RowInfo rowInfo = null;
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			rs = st.executeQuery("select a.seq, NRROWS, statement, from_statement from " + opt.logTable
					+ " a where a.seq = " + seq);
			while (rs.next()) {
				if (rowInfo != null) {
					throw new IllegalArgumentException("RowInfo has already been assigned to a value. This should not happen.");
				}
				rowInfo = new RowInfo(rs, true);
			}
			rs.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return rowInfo;
	}
	
	private Set<Long> getAllSequencesFromTable(String tableName) {
		Set<Long> res = new HashSet<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			rs = st.executeQuery("select distinct seq from " + tableName);
			while (rs.next()) {
				res.add(rs.getLong("SEQ"));
			}
			rs.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return res;
	}
	
	public static List<RowInfo> getAllProblematicStatements(OptionsOwn opt) {
		return INSTANCE.getAllStatementsFromTable(opt, QRS_PROBLEMATIC_SEQUENCES);
	}
	
	public static Set<Long> getAllStringTupleSequences() {
		return INSTANCE.getAllSequencesFromTable(QRS_QUERY_TUPLE_STRING);
	}
	
	public static Set<Long> getAllNumericTupleSequences() {
		return INSTANCE.getAllSequencesFromTable(QRS_QUERY_TUPLE_NUMERIC);
	}
	
	public static Set<Long> getAllTupleSequences() {
		Set<Long> sequences = new HashSet<>();
		sequences.addAll(getAllNumericTupleSequences());
		sequences.addAll(getAllStringTupleSequences());
		return sequences;
	}
	
	public static Set<Long> getAllProblematicSequences() {
		return INSTANCE.getAllSequencesFromTable(QRS_PROBLEMATIC_SEQUENCES);
	}
	
	public static int getTupleCount(long seq) {
		int count = -1;
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			//NUMERIC
			rs = st.executeQuery("select count(*) as count from " + QRS_QUERY_TUPLE_NUMERIC + " a where a.seq = " + seq);
			while (rs.next()) {
				count = rs.getInt("count");
			}
			if (count < 1) {
				//STRING
				rs = st.executeQuery("select count(*) as count from " + QRS_QUERY_TUPLE_STRING + " a where a.seq = " + seq);
				while (rs.next()) {
					count = rs.getInt("count");
				}
			}
			rs.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return count;
	}
	
	public static List<Pair<Long, Integer>> getTupleFrequency() {
		List<Pair<Long, Integer>> frequencyPairs = new ArrayList<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			//NUMERIC
			rs = st.executeQuery("select seq, count(*) as count from " + QRS_QUERY_TUPLE_NUMERIC + " group by seq order by seq");
			while (rs.next()) {
				frequencyPairs.add(new Pair<>(rs.getLong("seq"), rs.getInt("count")));
			}
			//STRING
			rs = st.executeQuery("select seq, count(*) as count from " + QRS_QUERY_TUPLE_STRING + " group by seq order by seq");
			while (rs.next()) {
				frequencyPairs.add(new Pair<>(rs.getLong("seq"), rs.getInt("count")));
			}
			rs.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return frequencyPairs;
	}
	
	public static Set<Long> transformRowIntoToSequences(Collection<RowInfo> rowInfos) {
		Set<Long> sequences = new HashSet<>();
		for (RowInfo rowInfo : rowInfos) {
			sequences.add(rowInfo.seq);
		}
		return sequences;
	}

	public static Set<TupleInfo> getAllTuples(long seq) {
		Set<TupleInfo> res = new HashSet<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(500000);
			ResultSet rs = null;
			//NUMERIC
			rs = st.executeQuery("select seq, table_id, key_id from " + QRS_QUERY_TUPLE_NUMERIC + " a where a.seq = " + seq
					+ " order by seq");
			while (rs.next()) {
				res.add(new TupleInfo(rs, false));
			}
			//STRING
			rs = st.executeQuery("select seq, table_id, key_id from " + QRS_QUERY_TUPLE_STRING + " a where a.seq = " + seq
					+ " order by seq");
			while (rs.next()) {
				res.add(new TupleInfo(rs, true));
			}
			rs.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return res;
	}

	public static Set<Long> getSimilarSequences(TupleInfo tuple) {
		Set<Long> similarSequences = new HashSet<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(500000);
			ResultSet resultSet = null;
			String tableName = QRS_QUERY_TUPLE_NUMERIC;
			if (tuple.isKeyString()) {
				tableName = QRS_QUERY_TUPLE_STRING;
			}
			resultSet = st
					.executeQuery("select seq from " + tableName + " a where not a.seq = " + tuple.getSequence()
							+ "" + " and a.table_id = " + tuple.getTableId() + " and a.key_id = " + tuple.getKeyId());
			while (resultSet.next()) {
				similarSequences.add(resultSet.getLong("SEQ"));
			}
			resultSet.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return similarSequences;
	}

	public static boolean saveTableToDBWithNoDuplicates(List<Pair<Table, Object>> queryResult, RowInfo ri) {
		Set<Pair<Table, Object>> querySet = new HashSet<>(queryResult);
		if (querySet.isEmpty()) {
			try {
				Statement st = conn.createStatement();
				String tableID = QRS_PROCESSED_STRAYS;
				String query = "INSERT INTO " + tableID + " ( SEQ ) SELECT  " + ri.seq
						+ " FROM dual WHERE NOT EXISTS ( SELECT 1 FROM " + tableID + " WHERE SEQ = " + ri.seq + " )";
				System.out.println("Saving ProcessedStrays: " + ri);
				st.executeQuery(query);
				conn.commit();
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return false;
		}
		boolean errorRidden = false;
		System.out.println("Executing: " + ri);
		for (Pair<Table, Object> tuple : querySet) {
			Table table = tuple.getFirst();
			Object keyId = tuple.getSecond();
			String stringAddition = "";
			String tableID = QRS_QUERY_TUPLE_NUMERIC;
			try {
				Statement st = conn.createStatement();
				if (table.keyColumn.attributeType == 3) {
					// means that the key of the table is of type String
					tableID = QRS_QUERY_TUPLE_STRING;
					stringAddition = "\'";
				}
				String query = "INSERT INTO " + tableID + " ( SEQ, TABLE_ID, KEY_ID ) SELECT  " + ri.seq + ", "
						+ table.tableId + ", " + stringAddition + keyId + stringAddition
						+ " FROM dual WHERE NOT EXISTS (" + " SELECT 1 FROM " + tableID + " WHERE SEQ = " + ri.seq
						+ " AND TABLE_ID = " + table.tableId + " AND KEY_ID = " + stringAddition + keyId
						+ stringAddition + " )";
				st.executeQuery(query);
				conn.commit();
				st.close();
			} catch (Exception e) {
				e.printStackTrace();
				errorRidden = true;
				// FIXME should we really break here?
				break;
			}
		}
		if (errorRidden) {
			saveProblematicSequencesDB(ri);
		}
		return errorRidden;
	}
	
	public static boolean saveTableToDB(List<Pair<Table, Object>> queryResult, RowInfo ri) {
		return saveTableToDummyDB(queryResult, ri, "");
	}
	
	public static boolean saveTableToDummyDB(List<Pair<Table, Object>> queryResult, RowInfo ri, String dummyName) {
		Set<Pair<Table, Object>> querySet = new HashSet<>(queryResult);
		if (querySet.isEmpty()) {
			try {
				Statement st = conn.createStatement();
				String tableID = QRS_PROCESSED_STRAYS;
				String query = "INSERT INTO " + tableID + " ( SEQ ) SELECT  " + ri.seq
						+ " FROM dual WHERE NOT EXISTS ( SELECT 1 FROM " + tableID + " WHERE SEQ = " + ri.seq + " )";
				System.out.println("Saving ProcessedStrays: " + ri);
				st.executeQuery(query);
				conn.commit();
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return false;
		}
		boolean errorRidden = false;
		System.out.println("Hi Executing: " + ri);
		PreparedStatement preparedStatement = null;
		boolean previousAutoCommit = false;
		String tableID = QRS_QUERY_TUPLE_NUMERIC + dummyName;
		try {
			preparedStatement = conn.prepareStatement("INSERT INTO " + tableID + " ( SEQ, TABLE_ID, KEY_ID ) values (" + ri.seq 
					+ ", ?, ?)");
			previousAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1.getMessage());
		}
		int i = 0;
		List<Pair<Table, Object>> tuplesForStringTable = new ArrayList<>();
		for (Pair<Table, Object> tuple : querySet) {
			i++;
			Table table = tuple.getFirst();
			Object keyId = tuple.getSecond();
			try {
				if (table.keyColumn.attributeType == 3) {
					tuplesForStringTable.add(tuple);
				} else {
					preparedStatement.setInt(1, table.tableId);
					preparedStatement.setLong(2, Long.parseLong((String) keyId));
//					preparedStatement.setInt(3, table.tableId);
//					preparedStatement.setLong(4, Long.parseLong((String) keyId));
					preparedStatement.addBatch();
					if (i % 1000 == 0) {
						preparedStatement.executeBatch();
						conn.commit();
						System.out.println("Committed to: " + tableID);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				errorRidden = true;
				// FIXME should we really break here?
				break;
			}
		}
		if (i % 1000 != 0) {
			try {
				preparedStatement.executeBatch();
				conn.commit();
				System.out.println("Committed to: " + tableID);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		if (!tuplesForStringTable.isEmpty()) {
			tableID = QRS_QUERY_TUPLE_STRING + dummyName;
			try {
				preparedStatement = conn.prepareStatement("INSERT INTO " + tableID + " ( SEQ, TABLE_ID, KEY_ID ) values (" + ri.seq 
						+ ", ?, ?)");
				previousAutoCommit = conn.getAutoCommit();
				conn.setAutoCommit(false);
			} catch (SQLException e1) {
				e1.printStackTrace();
				throw new RuntimeException(e1.getMessage());
			}
			i = 0;
			for (Pair<Table, Object> tuple : tuplesForStringTable) {
				i++;
				Table table = tuple.getFirst();
				Object keyId = tuple.getSecond();
				try {
					preparedStatement.setInt(1, table.tableId);
					preparedStatement.setString(2, (String) keyId);
					System.out.println("PreparedStatement: " + preparedStatement.getMetaData());
					preparedStatement.addBatch();
					if (i % 1000 == 0) {
						preparedStatement.executeBatch();
						conn.commit();
						System.out.println("Committed to: " + tableID);
					}
				} catch (Exception e) {
					e.printStackTrace();
					errorRidden = true;
					// FIXME should we really break here?
					break;
				}
			}
			if (i % 1000 != 0) {
				try {
					preparedStatement.executeBatch();
					conn.commit();
					System.out.println("Committed to: " + tableID);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		try {
			preparedStatement.close();
			conn.setAutoCommit(previousAutoCommit);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
		if (errorRidden) {
			saveProblematicSequencesDB(ri);
		}
		return errorRidden;
	}
	
	public static void saveComparableSequences() {
		
		Statement st;
		try {
			st = conn.createStatement();
			String query = "insert into " + QRS_COMPARABLE_SEQUENCES + "(seq) select distinct seq "
					+ "from (select * from (select table_id, key_id, count(*) as count from "
					+ QRS_QUERY_TUPLE_STRING + " group by table_id, key_id) "
					+ "where count > 1) a join " + QRS_QUERY_TUPLE_STRING + " b  on a.key_id = b.key_id and a.table_id = b.table_id";
			st.executeQuery(query);
			conn.commit();
			st.close();
			st = conn.createStatement();
			query = "insert into " + QRS_COMPARABLE_SEQUENCES + "(seq) select distinct seq "
					+ "from (select * from (select table_id, key_id, count(*) as count from "
					+ QRS_QUERY_TUPLE_NUMERIC + " group by table_id, key_id) "
					+ "where count > 1) a join " + QRS_QUERY_TUPLE_NUMERIC + " b  on a.key_id = b.key_id and a.table_id = b.table_id";
			st.executeQuery(query);
			conn.commit();
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void saveFixedStatementsToDB(List<Pair<Table, Object>> queryResult, RowInfo ri) {
		boolean errorRidden = saveTableToDBWithNoDuplicates(queryResult, ri);
		if (errorRidden) {
			System.out.println("ERROR SEQ: " + ri);
		} else {
			String deleteQuery = "DELETE FROM QRS_PROBLEMATIC_SEQUENCES WHERE seq = " + ri.seq;
			Statement st;
			try {
				st = conn.createStatement();
				st.executeQuery(deleteQuery);
				conn.commit();
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static void saveProblematicSequencesDB(RowInfo ri) {
		try {
			Statement st = conn.createStatement();
			String tableID = QRS_PROBLEMATIC_SEQUENCES;
			String query = "INSERT INTO " + tableID + " ( SEQ ) SELECT  " + ri.seq
					+ " FROM dual WHERE NOT EXISTS ( SELECT 1 FROM " + tableID + " WHERE SEQ = " + ri.seq + " )";
			System.out.println("Saving ProblematicSequence: " + ri);
			st.executeQuery(query);
			conn.commit();
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static Set<Long> getStrayQueries(OptionsOwn opt) {
		Set<Long> result = new HashSet<>();
		
		Set<Long> knownQueries = new HashSet<>();
		knownQueries.addAll(getAllProblematicSequences());
		knownQueries.addAll(getAllStringTupleSequences());
		knownQueries.addAll(getAllNumericTupleSequences());
		knownQueries.addAll(INSTANCE.getAllSequencesFromTable(QRS_PROCESSED_STRAYS));
		
		for (Long seq : INSTANCE.getAllSequencesFromTable(opt.logTable)) {
			if (!knownQueries.contains(seq)) {
				result.add(seq);
			}
		}
		
		return result;
	}
	
	public static Set<RowInfo> getStrayRowInfos(OptionsOwn opt) {
		Set<RowInfo> result = new HashSet<>();
		
		Set<Long> knownQueries = new HashSet<>();
		knownQueries.addAll(getAllProblematicSequences());
		knownQueries.addAll(getAllStringTupleSequences());
		knownQueries.addAll(getAllNumericTupleSequences());
		knownQueries.addAll(INSTANCE.getAllSequencesFromTable(QRS_PROCESSED_STRAYS));
		
		for (RowInfo rowInfo : getAllRelevantStatements(opt)) {
			if (rowInfo.seq == 17603537 || rowInfo.seq == 17603493 || rowInfo.seq == 17588010 || rowInfo.seq == 17605732 || !knownQueries.contains(rowInfo.seq)) {
				result.add(rowInfo);
			}
		}
		
		return result;
	}

	public static String getPrimaryColumnName(String tableName) {
		String primaryColumnName = null;
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet resultSet = st
					.executeQuery("select column_name from " + QRS_DB_SCHEMA + " where table_name = \'" + tableName + "\' "
							+ "and is_key is not null");
			while (resultSet.next()) {
				if (primaryColumnName != null) {
					System.out.println("CurrentColumnNAme: " + primaryColumnName);
					System.out.println("New: " + resultSet.getString("column_name"));
					throw new IllegalArgumentException("ResultSet returns more than 1 result! Should not be possible.");
				}
				primaryColumnName = resultSet.getString("column_name");
			}
			resultSet.close();
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return primaryColumnName;
	}

}
