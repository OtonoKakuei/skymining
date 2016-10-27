package largespace.business;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import aima.core.util.datastructure.Pair;

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
	public List<Long> GetlastSeq(String tableLastSeq, String tableLog) {
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

	public HashMap<String, Table> GetTablesKeys() {
		HashMap<String, Table> map = new HashMap<String, Table>();
		try {

			Statement st = conn.createStatement();
			ResultSet rs = null;

			rs = st.executeQuery(
					"SELECT TABLE_ID, TABLE_NAME,    COLUMN_NAME,    COLUMN_TYPE, ATTRIBUTE_ID, IS_KEY FROM QRS_DB_SCHEMA WHERE IS_KEY = 1");
			while (rs.next()) {
				Table t = new Table(rs, true);

				map.put(t.Name, t);
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

	public void SetlastSeq(Long lastSeq, String tableLastSeq) {
		try {
			Statement st = conn.createStatement();
			ResultSet rs = null;

			rs = st.executeQuery("UPDATE " + tableLastSeq + " SET LAST_SEQ = " + lastSeq.toString());

			conn.commit();
			rs.close();
			st.close();
		} catch (Exception ex) {
			System.out.println("Exception in GetlastSeq, ex = " + ex.getMessage());
		}

	}

	public void SaveTableToDB(List<Pair<Table, Object>> queryResult, RowInfo ri) {
		// TODO save the result of the query with seq = seq to our internal DB
		// you need to implement this
	}
}
