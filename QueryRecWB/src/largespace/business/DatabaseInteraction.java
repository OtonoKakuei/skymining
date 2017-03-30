package largespace.business;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import aima.core.util.datastructure.Pair;
import wb.model.OrderingType;
import wb.model.SessionInfo;
import wb.model.TupleInfo;

/**
 * This class is responsible for all the interactions we do with the database.
 * Implemented here are (but not only): the core of the query similarity function as well as the session similarity function.
 * 
 * @author OtonoKakuei
 */
public class DatabaseInteraction {
	/**
	 * 
	 * The following are table names that we use in our code
	 * Not all of them are included here, some are only used as a local variable
	 */
	private static final String QRS_US_SEGMENTED = "QRS_US_SEGMENTED";
	private static final String QRS_SIMILAR_SEQS = "QRS_SIMILAR_SEQS";
	private static final String QRS_QUERY_SIMILARITY = "QRS_QUERY_SIMILARITY";
	private static final String QRS_PROCESSED_STRAYS = "QRS_PROCESSED_STRAYS";
	private static final String QRS_DB_SCHEMA = "QRS_DB_SCHEMA";
	private static final String QRS_QUERY_TUPLE_STRING = "QRS_QUERY_TUPLE_STRING";
	private static final String QRS_QUERY_TUPLE_NUMERIC = "QRS_QUERY_TUPLE_NUMERIC";
	private static final String QRS_PROBLEMATIC_SEQUENCES = "QRS_PROBLEMATIC_SEQUENCES";
	private static final String QRS_COMPARABLE_SEQUENCES = "QRS_COMPARABLE_SEQUENCES";
	
	private static final Map<OrderingType, Map<Long, SessionInfo>> SESSION_INFO_ORDER_MAP = new HashMap<>();
	private static final Map<Pair<Long, Long>, Float> QUERY_SIMILARITY_MAP = new HashMap<>();
	
	public static Connection conn;
	
	private static final DatabaseInteraction INSTANCE = new DatabaseInteraction();
	
	private DatabaseInteraction() {
		
	}
	
	/**
	 * Establishes the connection with the database, based on the configuration stored in {@link OptionsOwn}
	 * @param opt configuration for the DB connection
	 */
	public static void establishConnection(OptionsOwn opt) {
		establishConnection(opt.serverAddress, opt.username, opt.password);
	}
	
	/**
	 * Establishes a connection with the database
	 * @param serverAddress address of the servre
	 * @param username user name
	 * @param password password
	 */
	public static void establishConnection(String serverAddress, String username, String password) {
		// Establish connection
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			conn = DriverManager.getConnection("jdbc:oracle:thin:@" + serverAddress, username, password);
			conn.setAutoCommit(false);
			System.out.println("Connection established");

		} catch (Exception e) {
			System.err.println("Could not establish Connection");
			e.printStackTrace();
		}
	}

	/**
	 * Closes the connection with the database.
	 */
	public static void closeConnection() {
		try {
			conn.close();
			System.out.println("Connection closed");

		} catch (Exception e) {
			System.err.println("Could not close Connection");
		}
	}

	/**
	 * Returns all the keys from all the existing tables that are stored in the QRS_DB_SCHEMA table
	 * @return all the keys from all the existing tables that are stored in the QRS_DB_SCHEMA table
	 */
	public static HashMap<String, Table> getTablesKeys() {
		HashMap<String, Table> map = new HashMap<>();
		try {
			Statement st = conn.createStatement();
			ResultSet rs = null;

			rs = st.executeQuery(
					"SELECT TABLE_ID, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, ATTRIBUTE_ID, IS_KEY FROM QRS_DB_SCHEMA WHERE IS_KEY = 1");
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

	
	/**
	 * Gets all statements that have at least 1 tuple.
	 * @param opt stores the name of the log table
	 * @return all statements that have at least 1 tuple.
	 */
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
	
	/**
	 * A helpful method to extract all the statements from a given table. Will only work if the
	 * table has the correct columns.
	 * @param opt stores the name of the log table
	 * @param tableName name of the given table
	 * @return all the statements from a given table.
	 */
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
	
	/**
	 * This method extracts and returns the {@link RowInfo} of a given sequence from the log table.
	 * @param opt stores the name of the log table
	 * @param seq the given sequence number
	 * @return the {@link RowInfo} of a given sequence from the log table. 
	 */
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
	
	/**
	 * Returns all unique sequences from a given table. The table has to have to the column "SEQ" for this to succeed.
	 * @param tableName the given table name.
	 * @return all unique sequences from a given table.
	 */
	private List<Long> getAllSequencesFromTable(String tableName) {
		List<Long> res = new ArrayList<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			rs = st.executeQuery("select distinct seq from " + tableName + " order by seq");
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
	
	//The followings are the restricted implementations for the getAllSequencesFromTable()
	//It is ensured that each methods are only performed on tables with the SEQ column.
	//Add a new static method if you want to get the sequences from a new table.
	public static List<Long> getAllComparableSequences() {
		return INSTANCE.getAllSequencesFromTable(QRS_COMPARABLE_SEQUENCES);
	}
	
	public static List<Long> getAllProcessedSimilarSequences() {
		return INSTANCE.getAllSequencesFromTable(QRS_SIMILAR_SEQS);
	}
	
	public static List<Long> getAllSequencesFromQuerySimilarity() {
		return INSTANCE.getAllSequencesFromTable(QRS_QUERY_SIMILARITY);
	}
	
	
	public static List<Long> getAllStringTupleSequences() {
		return INSTANCE.getAllSequencesFromTable(QRS_QUERY_TUPLE_STRING);
	}
	
	public static List<Long> getAllNumericTupleSequences() {
		return INSTANCE.getAllSequencesFromTable(QRS_QUERY_TUPLE_NUMERIC);
	}
	
	public static List<Long> getAllProblematicSequences() {
		return INSTANCE.getAllSequencesFromTable(QRS_PROBLEMATIC_SEQUENCES);
	}
	
	//this one's similar to above, with the difference that it gets the RowInfos instead of the sequence numbers
	public static List<RowInfo> getAllProblematicStatements(OptionsOwn opt) {
		return INSTANCE.getAllStatementsFromTable(opt, QRS_PROBLEMATIC_SEQUENCES);
	}
	/**
	 * Returns all the unique sequences from all tuples (numeric and string tuples).
	 * @return all the unique sequences from all tuples (numeric and string tuples).
	 */
	public static Set<Long> getAllTupleSequences() {
		Set<Long> sequences = new HashSet<>();
		sequences.addAll(getAllNumericTupleSequences());
		sequences.addAll(getAllStringTupleSequences());
		return sequences;
	}
	
	
	/**
	 * Counts the total number of all the tuples from a given sequence number.
	 * @param seq the given sequence number
	 * @return the total number of all the tuples from a given sequence number.
	 */
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
	
	/**
	 * Returns the number of tuples per sequences.
	 * @return the number of tuples per sequences.
	 */
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
	
	
	/**
	 * Extracts the sequence numbers from the list of {@link RowInfo}.
	 * @param rowInfos the given list of RowInfos.
	 * @return the sequence numbers from the list of {@link RowInfo}.
	 */
	public static Set<Long> transformRowIntoToSequences(Collection<RowInfo> rowInfos) {
		Set<Long> sequences = new HashSet<>();
		for (RowInfo rowInfo : rowInfos) {
			sequences.add(rowInfo.seq);
		}
		return sequences;
	}

	/**
	 * Returns all the tuples of the given sequence number.
	 * @param seq the given sequence number.
	 * @return all the tuples of the given sequence number.
	 */
	public static Set<TupleInfo> getAllTuples(long seq) {
		Set<TupleInfo> res = new HashSet<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet rs = null;
			//NUMERIC
			rs = st.executeQuery("select seq, table_id, key_id from " + QRS_QUERY_TUPLE_NUMERIC + " a where a.seq = " + seq);
			while (rs.next()) {
				res.add(new TupleInfo(rs, false));
			}
			if (res.isEmpty()) {
				//STRING
				rs = st.executeQuery("select seq, table_id, key_id from " + QRS_QUERY_TUPLE_STRING + " a where a.seq = " + seq);
				while (rs.next()) {
					res.add(new TupleInfo(rs, true));
				}
			}
			rs.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return res;
	}
	
	/**
	 * Returns all the {@link SessionInfo} according to the way the sequences were ordered.
	 * @param orderType describes in what order the sequences were executed.
	 * @return all the {@link SessionInfo} according to the way the sequences were ordered.
	 */
	public static Set<SessionInfo> getAllSessions(OrderingType orderType) {
		INSTANCE.initNewOrderSessionMap(orderType);
		Map<Long, SessionInfo> sessionInfoMap = SESSION_INFO_ORDER_MAP.get(orderType);
		
		String addition = "_" + orderType;
		Set<SessionInfo> res = new HashSet<>();
		Map<Long, Map<Integer, Long>> sessionsMap = new HashMap<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet resultSet = null;
			resultSet = st.executeQuery("select user_session, seq, index_position from QRS_US_SEGMENTED" + addition + " where full_session = 1");
			while (resultSet.next()) {
				long sequence = -1L;
				long session = -1L;
				int indexPosition;
				try {
					sequence = resultSet.getLong("SEQ");
					session = resultSet.getLong("USER_SESSION");
					indexPosition = resultSet.getInt("INDEX_POSITION");
					Map<Integer, Long> sequencesMap = sessionsMap.get(session);
					if (sequencesMap == null) {
						sequencesMap = new HashMap<>();
						sessionsMap.put(session, sequencesMap);
					}
					sequencesMap.put(indexPosition, sequence);
				} catch (Exception ex) {
					System.out.println("Exception in Getting data, session = " + session + " seq = " + sequence);
				}
			}
			for (Entry<Long, Map<Integer, Long>> entry : sessionsMap.entrySet()) {
				Long sessionId = entry.getKey();
				Map<Integer, Long> sequencesMap = entry.getValue();
				List<Long> sequences = new ArrayList<>();
				for (int i = 1; i <= sequencesMap.size(); i++) {
					sequences.add(sequencesMap.get(i));
				}
				SessionInfo sessionInfo = new SessionInfo(sessionId, sequences);
				res.add(sessionInfo);
				
				sessionInfoMap.put(sessionId, sessionInfo);
			}
			resultSet.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return res;
	}
	
	/**
	 * Just a helper method to initialize the session info map that is based on the given {@link OrderingType}
	 * @param orderType describes in what order the sequences were executed.
	 */
	private void initNewOrderSessionMap(OrderingType orderType) {
		if (SESSION_INFO_ORDER_MAP.get(orderType) == null) {
			SESSION_INFO_ORDER_MAP.put(orderType, new HashMap<>());
		}
	}
	
	/**
	 * Segments the sequences of all the given {@link SessionInfo} and store the sequence and session pairs
	 * inside QRS_US_SEGMENTED table.
	 * @param sessions the given {@link SessionInfo}
	 */
	public static void segmentAndInsertUserSessions(Set<SessionInfo> sessions) {
		//comment or delete time variables if you don't need to know those info.
		int index = 1;
		int seqsCount = 1;
		Long beginTime = System.currentTimeMillis();
		double maxTime = 0.0;
		double totalTime = 0.0;
		double averageTime = 0.0;
		PreparedStatement preparedStatement = null;
		boolean previousAutoCommit = false;
		try {
			preparedStatement = conn.prepareStatement("INSERT INTO " + QRS_US_SEGMENTED + " ( USER_SESSION, SEQ, LAST_SEQ, LAST_POSITION, FULL_SESSION )"
					+ " SELECT ?, ?, ?, ?, ? FROM dual");
			previousAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1.getMessage());
		}
		for (SessionInfo session : sessions) {
			
			System.out.println(index + "/" + sessions.size());
			
			long sessionId = session.getSessionId();
			List<Long> orderedSequences = session.getOrderedSequences();
			
			for (int i = 0; i < orderedSequences.size(); i++) {
				int lastPosition = i + 1;
				long lastSeq = orderedSequences.get(i);
				boolean isFinalSeq = false;
				if (i == orderedSequences.size() - 1) {
					isFinalSeq = true;
				}
				for (int j = 0; j <= i; j++) {
					long seq = orderedSequences.get(j);
					try {
						preparedStatement.setLong(1, sessionId);
						preparedStatement.setLong(2, seq);
						preparedStatement.setLong(3, lastSeq);
						preparedStatement.setLong(4, lastPosition);
						preparedStatement.setString(5, (isFinalSeq ? 1 : 0) + "");
						preparedStatement.addBatch();
						if (seqsCount % 1000 == 0) {
							System.out.println("Trying to execute batch");
							preparedStatement.executeBatch();
							conn.commit();
							System.err.println("TotalTime: " + totalTime + ", AverageTime: " + averageTime + ", MaxTime: " + maxTime);
							System.out.println("Committed to: " + QRS_US_SEGMENTED);
						}
						seqsCount++;
					} catch (Exception e) {
						e.printStackTrace();
						break;
					}
				}
			}
			
			double timeNeeded = ((System.currentTimeMillis() - beginTime)) / 1000.0;
			totalTime += timeNeeded;
			averageTime = totalTime / index;
			if (maxTime < timeNeeded) {
				maxTime = timeNeeded;
			}
			index++;
		}
		//pushes the rest of the stuff i
		if (seqsCount % 1000 != 0) {
			try {
				preparedStatement.executeBatch();
				conn.commit();
				System.out.println("Committed to: " + QRS_US_SEGMENTED);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		try {
			preparedStatement.close();
			conn.setAutoCommit(previousAutoCommit);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
	
	/**
	 * Finds all sessions that are similar to each segmented sessions. This approach does not consider the order
	 * of the sequences inside the similar sessions, and thus, all the sequences inside those similar sessions
	 * will be returned.
	 * @param bestSessionCount the number (<b>N</b>) of similar sessions that will be returned. Only the <b>N</b> most similar ones are returned.
	 * @param orderType describes how the sequences inside the session is ordered.
	 */
	public static void processUnorderedSimilarity(int bestSessionCount, OrderingType orderType) {
		String addition = "_" + orderType;
		long beginTime = System.currentTimeMillis();
		System.out.println("Preparing!!");
		//get all similar queries
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet resultSet = null;
			resultSet = st.executeQuery("select * from QRS_QUERY_SIMILARITY");
			while (resultSet.next()) {
				QUERY_SIMILARITY_MAP.put(new Pair<>(resultSet.getLong("seq"),
						resultSet.getLong("other_seq")), resultSet.getFloat("similarity"));
			}
			resultSet.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		Set<Pair<Long, Long>> segmentedSessions = new HashSet<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet resultSet = null;
			resultSet = st.executeQuery("select sess, last_seq from qrs_us_similarity group by sess, last_seq");
			while (resultSet.next()) {
				try {
					segmentedSessions.add(new Pair<>(resultSet.getLong("SESS"), resultSet.getLong("LAST_SEQ")));
				} catch (Exception ex) {
					System.out.println("Exception in getting data");
				}
			}
			resultSet.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		//get all unordered similarity info
		//<<session id, last seq>, <other session, similarity>>
		Map<Pair<Long, Long>, List<Pair<Long, Float>>> similarityInfoMap = new HashMap<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet resultSet = null;
			resultSet = st.executeQuery("select sess, last_seq, other_sess, unordered_similarity from qrs_us_similarity order by unordered_similarity desc");
			while (resultSet.next()) {
				try {
					long currentSession = resultSet.getLong("sess");
					long currentSeq = resultSet.getLong("last_seq");
					long otherSession = resultSet.getLong("other_sess");
					float similarity = resultSet.getFloat("unordered_similarity");
					Pair<Long, Long> segmentedSession = new Pair<>(currentSession, currentSeq);
					List<Pair<Long, Float>> similarSessionInfos = similarityInfoMap.get(segmentedSession);
					if (similarSessionInfos == null) {
						similarSessionInfos = new ArrayList<>();
						similarityInfoMap.put(segmentedSession, similarSessionInfos);
					}
					similarSessionInfos.add(new Pair<>(otherSession, similarity));
				} catch (Exception ex) {
					System.out.println("Exception in getting data");
				}
			}
			resultSet.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println("Took: " + (System.currentTimeMillis() - beginTime) / 1000 + " seconds");
		
		//prepared statement
		int index = 1;
		int seqsCount = 1;
		beginTime = System.currentTimeMillis();
		double maxTime = 0.0;
		double totalTime = 0.0;
		double averageTime = 0.0;
		PreparedStatement preparedStatement = null;
		boolean previousAutoCommit = false;
		try {
			preparedStatement = conn.prepareStatement("insert into qrs_ussf_unordered" + addition + " (current_session, other_session, last_seq,"
					+ " similarity, recommended_seq) values (?, ?, ?, ?, ?)");
			previousAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1.getMessage());
		}
		
		int segmentedSessionsSize = segmentedSessions.size();
		for (Pair<Long, Long> segmentedSession : segmentedSessions) {
			System.out.println(index + "/" + segmentedSessionsSize);
			List<Pair<Long, Float>> similarSessions = similarityInfoMap.get(segmentedSession);
			
			Collections.sort(similarSessions, new Comparator<Pair<Long, Float>>() {
				@Override
				public int compare(Pair<Long, Float> o1, Pair<Long, Float> o2) {
					return o2.getSecond().compareTo(o1.getSecond());
				}
			});
			
			List<Pair<Long, Float>> bestSessions = new ArrayList<>();
			for (Pair<Long, Float> session : similarSessions) {
				if (session.getSecond() < 1f && bestSessions.size() < bestSessionCount) {
					bestSessions.add(session);
				}
			}
			
			long userSession = segmentedSession.getFirst();
			long lastSeq = segmentedSession.getSecond();
			
			SessionInfo currentSession = findUserSession(userSession, orderType);
			int lastSeqIndex = currentSession.getOrderedSequences().indexOf(lastSeq);
			
			for (Pair<Long, Float> sessionInfo : bestSessions) {
				long otherSessionId = sessionInfo.getFirst();
				List<Long> recommendedSequences = new ArrayList<>(findUserSession(otherSessionId, orderType).getOrderedSequences());
				for (int j = 0; j <= lastSeqIndex; j++) {
					Long currentSeq = currentSession.getOrderedSequences().get(j);
					for (Iterator<Long> iter = recommendedSequences.iterator(); iter.hasNext();) {
						Long seq = iter.next();
						Float similarity = QUERY_SIMILARITY_MAP.get(new Pair<>(currentSeq, seq));
						if (similarity == null) {
							continue;
						}
						if (Math.abs(similarity - 1.f) < 1E-6) {
							iter.remove();
						}
					}
				}
				
				for (Long recommendedSeq : recommendedSequences) {
					try {
						preparedStatement.setLong(1, userSession);
						preparedStatement.setLong(2, otherSessionId);
						preparedStatement.setLong(3, lastSeq);
						Float unorderedSimilarity = sessionInfo.getSecond();
						preparedStatement.setFloat(4, unorderedSimilarity);
						preparedStatement.setLong(5, recommendedSeq);
						preparedStatement.addBatch();
						if (seqsCount % 1000 == 0) {
							System.out.println("Trying to execute batch");
							preparedStatement.executeBatch();
							conn.commit();
							System.err.println("TotalTime: " + totalTime + ", AverageTime: " + averageTime + ", MaxTime: " + maxTime);
							System.out.println("Committed to: QRS_USSF_UNORDERED" + addition);
						}
						seqsCount++;
					} catch (Exception e) {
						e.printStackTrace();
						break;
					}
				}
			}
			double timeNeeded = ((System.currentTimeMillis() - beginTime)) / 1000.0;
			totalTime += timeNeeded;
			averageTime = totalTime / index;
			if (maxTime < timeNeeded) {
				maxTime = timeNeeded;
			}
			index++;
		}
		if (seqsCount % 1000 != 0) {
			try {
				preparedStatement.executeBatch();
				conn.commit();
				System.out.println("Committed to: QRS_USSF_UNORDERED" + addition);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		try {
			preparedStatement.close();
			conn.setAutoCommit(previousAutoCommit);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
		
	}
	
	/**
	 * Finds the {@link SessionInfo} based on the given id of the user session, and what order the sequences
	 * in that user session was executed.
	 * @param userSessionId the id of the user session
	 * @param orderType defines how the sequences were ordered in the session
	 * @return
	 */
	public static SessionInfo findUserSession(long userSessionId, OrderingType orderType) {
		Map<Long, SessionInfo> sessionInfoMap = SESSION_INFO_ORDER_MAP.get(orderType);
		if (sessionInfoMap == null) {
			getAllSessions(orderType);
			sessionInfoMap = SESSION_INFO_ORDER_MAP.get(orderType);
			if (sessionInfoMap == null) {
				throw new IllegalArgumentException("Shouldn't be possible.");
			}
		}
		SessionInfo session = sessionInfoMap.get(userSessionId);
		if (session == null) {
			getAllSessions(orderType);
			session = SESSION_INFO_ORDER_MAP.get(orderType).get(userSessionId);
			if (session == null) {
				throw new IllegalArgumentException("Shouldn't be possible");
			}
		}
		return session;
	}
	
	/**
	 * Sets the expected sequences of each segmented sessions inside a given table.
	 * It is required that the table contains segmented sessions 
	 * @param tableName the name of the given table
	 * @param orderType how the sequences of each segmented sessions were ordered
	 */
	public static void updateExpectedSeq(String tableName, OrderingType orderType) {
		int index = 1;
		int seqsCount = 1;
		Long beginTime = System.currentTimeMillis();
		double maxTime = 0.0;
		double totalTime = 0.0;
		double averageTime = 0.0;
		PreparedStatement preparedStatement = null;
		boolean previousAutoCommit = false;
		try {
			preparedStatement = conn.prepareStatement("update " + tableName + " set expected_seq = ? where current_session = ? and last_seq = ?"
					+ " and seq_position = ?");
			previousAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1.getMessage());
		}
		Set<SessionInfo> sessions = getAllSessions(orderType);
		for (SessionInfo session : sessions) {
			
			System.out.println(index + "/" + sessions.size());
			
			long sessionId = session.getSessionId();
			List<Long> orderedSequences = session.getOrderedSequences();
			
			for (int i = 1; i < orderedSequences.size(); i++) {
				long lastSeq = orderedSequences.get(i - 1);
				long expectedSeq = orderedSequences.get(i);
				try {
					preparedStatement.setLong(1, expectedSeq);
					preparedStatement.setLong(2, sessionId);
					preparedStatement.setLong(3, lastSeq);
					preparedStatement.setLong(4, i);
					preparedStatement.addBatch();
					if (seqsCount % 1000 == 0) {
						System.out.println("Trying to execute batch");
						preparedStatement.executeBatch();
						conn.commit();
						System.err.println("TotalTime: " + totalTime + ", AverageTime: " + averageTime + ", MaxTime: " + maxTime);
						System.out.println("Committed to: " + tableName);
					}
					seqsCount++;
				} catch (Exception e) {
					e.printStackTrace();
					break;
				}
			}
			
			double timeNeeded = ((System.currentTimeMillis() - beginTime)) / 1000.0;
			totalTime += timeNeeded;
			averageTime = totalTime / index;
			if (maxTime < timeNeeded) {
				maxTime = timeNeeded;
			}
			index++;
		}
		if (seqsCount % 1000 != 0) {
			try {
				preparedStatement.executeBatch();
				conn.commit();
				System.out.println("Committed to: " + tableName);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		try {
			preparedStatement.close();
			conn.setAutoCommit(previousAutoCommit);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
	
	/**
	 * this method is not used anymore because it takes way too long to execute.
	 */
	@Deprecated
	public static void insertSegmentedUserSessionTuples() {
		try {
			Statement st = conn.createStatement();
			String query = "insert into qrs_us_segmented_tuple_string "
					+ "(user_session, last_seq, table_id, key_id, duplicate_count, full_session) select user_session, last_seq,"
					+ " table_id, key_id, count(*) as duplicate_count, full_session from  "
					+ "(select b.user_session as user_session, b.last_seq, a.table_id, a.key_id, b.full_session"
					+ " from QRS_QUERY_TUPLE_STRING a join  QRS_US_SEGMENTED b on a.seq = b.seq) "
					+ "group by user_session, last_seq, table_id, key_id, full_session order by user_session";
			st.executeQuery(query);
			
			query = "insert into qrs_us_segmented_tuple_numeric "
					+ "(user_session, last_seq, table_id, key_id, duplicate_count, full_session) select user_session, last_seq,"
					+ " table_id, key_id, count(*) as duplicate_count, full_session from  "
					+ "(select b.user_session as user_session, b.last_seq, a.table_id, a.key_id, b.full_session"
					+ " from QRS_QUERY_TUPLE_NUMERIC a join  QRS_US_SEGMENTED b on a.seq = b.seq) "
					+ "group by user_session, last_seq, table_id, key_id, full_session order by user_session";
			st.executeQuery(query);
			conn.commit();
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This method calculates the similarity between segmented sessions and "known sessions (aka full sessions)" without paying attention
	 * to the order of how the sequences were executed.
	 * @param orderType the order of how the sequences were executed in the sessions.
	 */
	public static void calculateUnorderedSimilarities(OrderingType orderType) {
		int index = 1;
		int seqsCount = 1;
		double totalTime = 0.0;
		double averageTime = 0.0;
		double maxTime = 0.0;
		PreparedStatement preparedStatement = null;
		boolean previousAutoCommit = false;
		String addition = "_" + orderType;
		try {
			preparedStatement = conn.prepareStatement("insert into QRS_US_SIMILARITY" + addition + " (sess, last_seq, seq_position, other_sess, unordered_similarity) select user_session, last_seq, seq_position, other_user_session, (total_intersect_number / (user_session_tuples + other_tuples - total_intersect_number)) as similarity from ("
					+ " select d.user_session, d.last_seq, d.seq_position, d.other_user_session, d.total_intersect_number, d.user_session_tuples, c.tuple_count as other_tuples from ("
					+ " select a.user_session, a.last_seq, a.other_user_session, a.total_intersect_number, b.last_position as seq_position, b.tuple_count as user_session_tuples from (select sess as user_session, last_seq, other_sess as other_user_session, sum(total_intersect_number) as total_intersect_number "
					+ " from (select temp.user_session1 as sess, temp.last_seq1 as last_seq, temp.user_session2 as other_sess, sum(count2 - (abs(count2 - count1) + count2 - count1) / 2) as total_intersect_number from (select q1.user_session as user_session1, q1.last_seq as last_seq1, q2.user_session as user_session2, q1.duplicate_count as count1, q2.duplicate_count as count2 from (select user_session, last_seq, table_id, key_id, count(*) as duplicate_count from (select b.user_session as user_session, b.last_seq, a.table_id, a.key_id from QRS_QUERY_TUPLE_numeric a join (select * from QRS_US_SEGMENTED" + addition + " where user_session = ? and last_seq = ?) b on a.seq = b.seq) group by user_session, last_seq, table_id, key_id order by user_session) q1  inner join QRS_US_TUPLE_NUMERIC q2 on q1.table_id = q2.table_id and q1.key_id = q2.key_id where q1.user_session != q2.user_session union (select q3.user_session as sess, q3.last_seq as last_seq, q4.user_session as other_sess, q3.duplicate_count as count1, q4.duplicate_count as count2 from (select user_session, last_seq, table_id, key_id, count(*) as duplicate_count from (select b.user_session as user_session, b.last_seq, a.table_id, a.key_id from QRS_QUERY_TUPLE_string a join (select * from QRS_US_SEGMENTED" + addition + " where user_session = ? and last_seq = ?) b on a.seq = b.seq) group by user_session, last_seq, table_id, key_id order by user_session) q3 inner join QRS_US_TUPLE_string q4 on q3.table_id = q4.table_id and q3.key_id = q4.key_id where q3.user_session != q4.user_session)) temp group by temp.user_session1, temp.last_seq1, temp.user_session2 order by temp.last_seq1) group by sess, last_seq, other_sess)"
					+ "  a"
					+ " join (select user_session, last_seq, last_position, sum(tuple_count) as tuple_count from (select user_session, last_seq, last_position, sum(duplicate_count) as tuple_count from (select user_session, last_seq, last_position, table_id, key_id, count(*) as duplicate_count from (select b.user_session as user_session, b.last_seq, b.last_position, a.table_id, a.key_id from QRS_QUERY_TUPLE_numeric a join (select * from QRS_US_SEGMENTED" + addition + " where user_session = ? and last_seq = ?) b on a.seq = b.seq) group by user_session, last_seq, last_position, table_id, key_id order by user_session) group by user_session, last_seq, last_position) group by user_session, last_seq, last_position"
					+ " union all (select user_session, last_seq, last_position, sum(tuple_count) as tuple_count from (select user_session, last_seq, last_position, sum(duplicate_count) as tuple_count from (select user_session, last_seq, last_position, table_id, key_id, count(*) as duplicate_count from (select b.user_session as user_session, b.last_seq, b.last_position, a.table_id, a.key_id from QRS_QUERY_TUPLE_string a join (select * from QRS_US_SEGMENTED" + addition + " where user_session = ? and last_seq = ?) b on a.seq = b.seq) group by user_session, last_seq, last_position, table_id, key_id order by user_session) group by user_session, last_seq, last_position) group by user_session, last_seq, last_position)"
					+ ") b on a.user_session = b.user_session and a.last_seq = b.last_seq"
					+ ") d join (select * from QRS_US_TUPLE_COUNT) c on d.other_user_session = c.user_session)");
			previousAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1.getMessage());
		}
		Set<SessionInfo> sessions = getAllSessions(orderType);
		for (SessionInfo session : sessions) {
			Long beginLoopTime = System.currentTimeMillis();
			System.out.println(index + "/" + sessions.size());
			
			long sessionId = session.getSessionId();
			List<Long> orderedSequences = session.getOrderedSequences();
			
			for (int i = 0; i < orderedSequences.size(); i++) {
				long lastSeq = orderedSequences.get(i);
				try {
					preparedStatement.setLong(1, sessionId);
					preparedStatement.setLong(2, lastSeq);
					preparedStatement.setLong(3, sessionId);
					preparedStatement.setLong(4, lastSeq);
					preparedStatement.setLong(5, sessionId);
					preparedStatement.setLong(6, lastSeq);
					preparedStatement.setLong(7, sessionId);
					preparedStatement.setLong(8, lastSeq);
					preparedStatement.addBatch();
					if (seqsCount % 1000 == 0) {
						preparedStatement.executeBatch();
						conn.commit();
						System.err.println("TotalTime: " + totalTime + ", AverageTime: " + averageTime + ", MaxTime: " + maxTime);
						System.out.println("Committed to: QRS_US_SIMILARITY" + addition);
					}
					seqsCount++;
				} catch (Exception e) {
					e.printStackTrace();
					break;
				}
			}
			
			double timeNeeded = ((System.currentTimeMillis() - beginLoopTime)) / 1000.0;
			totalTime += timeNeeded;
			averageTime = totalTime / index;
			if (maxTime < timeNeeded) {
				maxTime = timeNeeded;
			}
			index++;
		}
		if (seqsCount % 1000 != 0) {
			try {
				preparedStatement.executeBatch();
				conn.commit();
				System.out.println("Committed to: QRS_US_SIMILARITY" + addition);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		try {
			preparedStatement.close();
			conn.setAutoCommit(previousAutoCommit);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
	
	
	
	/**
	 * Returns sequence ids of sequences that are similar to the given sequence.
	 * Similar in this case means that the returned sequences have at least one intersecting witness/tuple
	 * with the witnesses/tuples of the given sequence.
	 * @param seq the given sequence id
	 * @return sequence ids of sequences that are similar to the given sequence.
	 */
	public static Set<Long> getSimilarSequencesFromTable(Long seq) {
		Set<Long> similarSequences = new HashSet<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet resultSet = null;
			String tableName = QRS_SIMILAR_SEQS;
			resultSet = st.executeQuery("select * from " + tableName + " a where a.seq = " + seq);
			while (resultSet.next()) {
				similarSequences.add(resultSet.getLong("OTHER_SEQ"));
			}
			resultSet.close();
			st.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return similarSequences;
	}
	
	/**
	 * Returns sequence ids of sequences that are similar to the given {@link TupleInfo}.
	 * Similar in this case means that the returned sequences have at least one intersecting witness/tuple
	 * with the given tuple.
	 * @param tuple the given {@link TupleInfo}.
	 * @return Returns sequence ids of sequences that are similar to the given {@link TupleInfo}.
	 */
	public static Set<Long> getSimilarSequences(TupleInfo tuple) {
		Set<Long> similarSequences = new HashSet<>();
		try {
			Statement st = conn.createStatement();
			st.setFetchSize(50000);
			ResultSet resultSet = null;
			String tableName = QRS_QUERY_TUPLE_NUMERIC;
			String stringAddition = "";
			if (tuple.isKeyString()) {
				tableName = QRS_QUERY_TUPLE_STRING;
				stringAddition = "'";
			}
			resultSet = st
					.executeQuery("select seq from " + tableName + " a where not a.seq = " + tuple.getSequence()
							+ "" + " and a.table_id = " + tuple.getTableId() + " and a.key_id = " + stringAddition + tuple.getKeyId() + stringAddition);
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

	/**
	 * Stores the pre-processed result into their respective tuple tables.
	 * Ensures that no duplicates are going to be added.
	 * <i>Has not been optimized with batches and prepared statement.</i>
	 * @param queryResult the result of the query execution.
	 * @param ri {@link RowInfo} contains the information of a query inside the query data log table.
	 * @return true if the tuples are added successfully, false otherwise.
	 */
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
				break;
			}
		}
		if (errorRidden) {
			saveProblematicSequencesDB(ri);
		}
		return errorRidden;
	}
	
	/**
	 * Stores the result of the pre-processed query into their respective tuple tables.
	 * @param queryResult the result of the query execution.
	 * @param ri {@link RowInfo} contains the information of a query inside the query data log table.
	 * @return true if the tuples are added successfully, false otherwise.
	 */
	public static boolean saveTableToDB(List<Pair<Table, Object>> queryResult, RowInfo ri) {
		return saveTableToDummyDB(queryResult, ri, "");
	}
	
	/**
	 * Stores query result in "dummy" tuple tables. The dummy table is basically a table with the same prefix
	 * as the original table and appended with a "dummy" suffix.
	 * @param queryResult the result of the query execution.
	 * @param ri {@link RowInfo} contains the information of a query inside the query data log table.
	 * @param dummyName the "dummy" suffix that is going to appended to the original tuple table names.
	 * @return true if the tuples are added successfully, false otherwise.
	 */
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
		System.out.println("Executing: " + ri);
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
	
	/**
	 * Stores all the sequences that can be compared.
	 * "Can be compared" means that they have at least 1 tuple.
	 */
	public static void saveComparableSequences() {
		Statement st;
		try {
			st = conn.createStatement();
			String query = "insert into " + QRS_COMPARABLE_SEQUENCES + " (seq) select distinct seq "
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
	
	/**
	 * Stores query similarity between the given sequence and other sequences in the map
	 * @param seq the given sequence
	 * @param similarityMap contains the other sequences that have a similarity with the given sequence,
	 * and their similarity rate
	 */
	public static void saveQuerySimilarity(Long seq, Map<Long, Double> similarityMap) {
		PreparedStatement preparedStatement;
		String query = "insert into " + QRS_QUERY_SIMILARITY + " (seq, other_seq, similarity) values (" + seq + ", ?, ?)";
		boolean previousAutoCommit = false;
		try {
			preparedStatement = conn.prepareStatement(query);
			previousAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1.getMessage());
		}
		int i = 0;
		for (Entry<Long, Double> entry : similarityMap.entrySet()) {
			i++;
			try {
				preparedStatement.setLong(1, entry.getKey());
				preparedStatement.setDouble(2, entry.getValue());
				preparedStatement.addBatch();
				if (i % 1000 == 0) {
					preparedStatement.executeBatch();
					conn.commit();
				}
			} catch (Exception e) {
				e.printStackTrace();
				break;
			}
		}
		if (i % 1000 != 0) {
			try {
				preparedStatement.executeBatch();
				conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		try {
			preparedStatement.close();
			conn.setAutoCommit(previousAutoCommit);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
	
	/**
	 * Stores pairs of similar sequences into QRS_SIMILAR_SEQS table.
	 * @param seq the given sequence
	 * @param similarSequences the sequences that are similar to the given sequence
	 */
	public static void saveSimilarSequences(Long seq, Set<Long> similarSequences) {
		PreparedStatement preparedStatement;
		String query = "insert into " + QRS_SIMILAR_SEQS + " (seq, other_seq) values (" + seq + ", ?)";
		boolean previousAutoCommit = false;
		try {
			preparedStatement = conn.prepareStatement(query);
			previousAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1.getMessage());
		}
		int i = 0;
		for (long otherSeq : similarSequences) {
			i++;
			try {
				preparedStatement.setLong(1, otherSeq);
				preparedStatement.addBatch();
				if (i % 1000 == 0) {
					preparedStatement.executeBatch();
					conn.commit();
				}
			} catch (Exception e) {
				e.printStackTrace();
				break;
			}
		}
		if (i % 1000 != 0) {
			try {
				preparedStatement.executeBatch();
				conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		try {
			preparedStatement.close();
			conn.setAutoCommit(previousAutoCommit);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

	/**
	 * This method is solely used to fix some of the problematic sequences stored inside QRS_PROBLEMATIC_SEQUENCES
	 * and store the tuples of the "fixed" sequences into their respective tuple tables.
	 * @param queryResult tuples of the given {@link RowInfo}
	 * @param ri the problematic {@link RowInfo}
	 */
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

	/**
	 * Stores the sequence of a given problematic {@link RowInfo} into QRS_PROBLEMATIC_SEQUENCES
 	 * @param ri the given {@link RowInfo}
	 */
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
	
	/**
	 * Returns all the sequences that have not been processed.
	 * @param opt configuration for the DB connection
	 * @return all the sequences that have not been processed.
	 */
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
	
	/**
	 * Returns all the {@link RowInfo}s that have not been processed.
	 * @param opt configuration for the DB connection
	 * @return all the {@link RowInfo}s that have not been processed.
	 */
	public static Set<RowInfo> getStrayRowInfos(OptionsOwn opt) {
		Set<RowInfo> result = new HashSet<>();
		
		Set<Long> knownQueries = new HashSet<>();
		knownQueries.addAll(getAllProblematicSequences());
		knownQueries.addAll(getAllStringTupleSequences());
		knownQueries.addAll(getAllNumericTupleSequences());
		knownQueries.addAll(INSTANCE.getAllSequencesFromTable(QRS_PROCESSED_STRAYS));
		
		for (RowInfo rowInfo : getAllRelevantStatements(opt)) {
			if (!knownQueries.contains(rowInfo.seq)) {
				result.add(rowInfo);
			}
		}
		
		return result;
	}

	/**
	 * Returns the column name that works as a unique identifier of a given table.
	 * @param tableName the given table name
	 * @return the column name that works as a unique identifier of a given table.
	 */
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
