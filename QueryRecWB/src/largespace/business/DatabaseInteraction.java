package largespace.business;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import aima.core.util.datastructure.Pair;
import wb.model.TupleInfo;

public class DatabaseInteraction {
	public static Connection conn;

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

	// get the last Seq processed from query log
	// tableLastSeq - table name where last processed seq stored
	// tableLog - table name where the log stored
	public List<Long> getlastSeq(String tableLastSeq, String tableLog) {
		List<Long> list = new ArrayList<Long>();
		Long lastSeq = new Long(0);
		Long maxVal = new Long(0);
		try {
			Statement st = conn.createStatement();
			ResultSet rs = null;

			rs = st.executeQuery("select count(*) from " + tableLastSeq);
			Integer rowCounts = 0;
			if (rs.next()) {
				rowCounts = rs.getInt(1);
			}

			if (rowCounts == 0) {
				st = conn.createStatement();
				rs = null;

				rs = st.executeQuery("select min(seq), max(seq)  from " + tableLog);

				if (rs.next()) {
					lastSeq = rs.getLong(1);
					maxVal = rs.getLong(2);
				}

				st = conn.createStatement();
				rs = null;
				rs = st.executeQuery("INSERT INTO " + tableLastSeq + " (LAST_SEQ, FINAL_SEQ  ) VALUES  ("
						+ lastSeq.toString() + ", " + maxVal.toString() + " )");
			} else {
				//FIXME check if rs is closed correctly
				rs.close();;
				rs = st.executeQuery("SELECT LAST_SEQ, FINAL_SEQ FROM " + tableLastSeq);

				if (rs.next()) {
					lastSeq = rs.getLong(1);
					maxVal = rs.getLong(2);
				}
			}

			conn.commit();
			rs.close();
			st.close();
			list.add(lastSeq);
			list.add(maxVal);
		} catch (Exception ex) {
			System.out.println("Exception in GetlastSeq, ex = " + ex.getMessage());
		}
		return list;
	}

	public HashMap<String, Table> getTablesKeys() {
		HashMap<String, Table> map = new HashMap<String, Table>();
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

		}
		return map;
	}

	public List<RowInfo> getNextNStatements(Long lastSeq, Long nextSeq, OptionsOwn opt) {
		List<RowInfo> res = new ArrayList<RowInfo>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			rs = st.executeQuery("select seq, NRROWS, statement from " + opt.logTable + " where seq > "
					+ lastSeq.toString() + " and seq <= " + nextSeq.toString()
					+ "AND LOWER(statement) NOT LIKE '%create table%' AND LOWER(statement) NOT LIKE 'declare %' order by seq");
			while (rs.next()) {
				RowInfo ri = new RowInfo(rs, true);
				res.add(ri);
			}
		} catch (Exception ex) {

		}
		return res;
	}

	public List<RowInfo> getAllRelevantStatements(OptionsOwn opt) {
		List<RowInfo> res = new ArrayList<RowInfo>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			rs = st.executeQuery("select seq, NRROWS, statement from " + opt.logTable + " where nrrows > 0 "
					+ "AND LOWER(statement) NOT LIKE '%create table%' AND LOWER(statement) NOT LIKE 'declare %' order by seq");
			while (rs.next()) {
				RowInfo ri = new RowInfo(rs, true);
				res.add(ri);
			}
		} catch (Exception ex) {

		}
		return res;
	}
	
	public List<RowInfo> getAllProblematicStatements(OptionsOwn opt) {
		List<RowInfo> res = new ArrayList<RowInfo>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			rs = st.executeQuery("select seq, NRROWS, statement from " + opt.logTable + "a join QRS_PROBLEMATIC_SEQUENCES b on " 
					+ "a.seq = b.seq order by seq");
			while (rs.next()) {
				RowInfo ri = new RowInfo(rs, true);
				res.add(ri);
			}
		} catch (Exception ex) {

		}
		return res;
	}
	
	public static List<TupleInfo> getAllTuples(long seq) {
		List<TupleInfo> res = new ArrayList<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			rs = st.executeQuery("select seq, table_id, key_id from QRS_QUERY_TUPLE_NUMERIC a where a.seq = " + seq + " order by seq");
			while (rs.next()) {
				res.add(new TupleInfo(rs));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return res;
	}
	
	public static Set<Long> getSimilarSequences(TupleInfo tuple) {
		Set<Long> similarSequences = new HashSet<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet resultSet = null;
			resultSet = st.executeQuery("select seq from QRS_QUERY_TUPLE_NUMERIC a where not a.seq = " + tuple.getSequence() + ""
					+ " and a.table_id = " + tuple.getTableId() + " and a.key_id = " + tuple.getKeyId());
			while (resultSet.next()) {
				similarSequences.add(resultSet.getLong("SEQ"));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return similarSequences;
	}
	
	public void setlastSeq(Long lastSeq, String tableLastSeq) {
		try {
			Statement st = conn.createStatement();
			ResultSet rs = null;
			System.out.println("LastSeq: " + lastSeq.toString());
			rs = st.executeQuery("UPDATE " + tableLastSeq + " SET LAST_SEQ = " + lastSeq.toString());

			conn.commit();
			rs.close();
			st.close();
		} catch (Exception ex) {
			System.out.println("Exception in GetlastSeq, ex = " + ex.getMessage());
		}

	}

	public void saveTableToDB(List<Pair<Table, Object>> queryResult, RowInfo ri) {
		Set<Pair<Table, Object>> querySet = new HashSet<>(queryResult);
		boolean errorRidden = false;
		for (Pair<Table, Object> tuple : querySet) {
			Table table = tuple.getFirst();
			Object keyId = tuple.getSecond();
			String queryTupleTableID = "QRS_QUERY_TUPLE";
			String stringTableID = queryTupleTableID + "_STRING";
			String stringAddition = "";
			String numericTableID = queryTupleTableID + "_NUMERIC";
			String tableID = numericTableID;
			try {
				Statement st = conn.createStatement();
				if (table.keyColumn.attributeType == 3) {
					//means that the key of the table is of type String
					tableID = stringTableID;
					stringAddition = "\'";
				}
				String query = "INSERT INTO " + tableID + " ( SEQ, TABLE_ID, KEY_ID ) SELECT  "
						+ ri.seq + ", " + table.tableId + ", " + stringAddition + keyId +stringAddition + " FROM dual WHERE NOT EXISTS ("
								+ " SELECT 1 FROM " + tableID + " WHERE SEQ = " + ri.seq + " AND TABLE_ID = " + table.tableId
								+ " AND KEY_ID = " + stringAddition + keyId + stringAddition + " )";
				System.out.println("Executing: " + ri);
				st.executeQuery(query);
				conn.commit();
			} catch (Exception e) {
				e.printStackTrace();
				errorRidden = true;
				//FIXME should we really break here?
				break;
			}
		}
		if (errorRidden) {
			saveProblematicSequencesDB(ri);
		}
		
	}
	
	public void saveProblematicSequencesDB(RowInfo ri) {
		try {
			Statement st = conn.createStatement();
			String tableID = "QRS_PROBLEMATIC_SEQUENCES";
			String query = "INSERT INTO " + tableID + " ( SEQ ) SELECT  "
					+ ri.seq + " FROM dual WHERE NOT EXISTS ( SELECT 1 FROM " + tableID
					+ " WHERE SEQ = " + ri.seq + " )";
			System.out.println("Saving ProblematicSequence: " + ri);
			st.executeQuery(query);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
		
}
