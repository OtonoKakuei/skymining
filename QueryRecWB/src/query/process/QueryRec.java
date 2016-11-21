package query.process;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import accessarea.AccessArea;
import accessarea.AccessAreaExtraction;
import aima.core.util.datastructure.Pair;
import largespace.business.DatabaseInteraction;
import largespace.business.HttpURLConnectionExt;
import largespace.business.OptionsOwn;
import largespace.business.RowInfo;
import largespace.business.Table;
import net.sf.jsqlparser.statement.select.FromItem;

public class QueryRec {
	private OptionsOwn opt;

	public QueryRec(OptionsOwn opt_) {
		opt = opt_;
		// dbI = new DatabaseInteraction();
		DatabaseInteraction.establishConnection(opt.serverAddress, opt.username, opt.password);
		opt.tablesWithKeys = DatabaseInteraction.getTablesKeys();
	}

	public void preprocess(OptionsOwn opt) {
		// read data piece by piece from the DB
		// parse statements
		// write pre-processed data to the DB

		AccessAreaExtraction extraction = new AccessAreaExtraction();

		try {
			// FIXME check whether this has to be done several times or not,
			// because of disconnections, etc.
			List<RowInfo> relevantRows = DatabaseInteraction.getAllRelevantStatements(opt);
			System.out.println("Number of relevant rows: " + relevantRows.size());
			for (RowInfo rowInfo : relevantRows) {
				// if (rowInfo.seq < 272354) {
				// continue;
				// }
				try {
					System.out.println("SEQ: " + rowInfo.seq);
					DatabaseInteraction.establishConnection(opt.serverAddress, opt.username, opt.password);
					AccessArea accessArea = extraction.extractAccessArea(rowInfo.statement);
					List<FromItem> fi = accessArea.getFrom();
					// now we have tables in the from clause of the
					// statement
					// for each table we now keyColumn
					Map<String, Table> tables = QueryUtil.getTablesWithKeysFromTheFromItemsOfStatement(fi, opt);

					// for each query from query log
					// perform a query to the DB (SkyServer)
					// internalDB is the DB (internal DB)
					HttpURLConnectionExt internalDB = new HttpURLConnectionExt();
					List<Pair<Table, Object>> queryResult = internalDB.sendGetResultFromQuery(rowInfo,
							(HashMap<String, Table>) tables);
					// store data to our internal DB
					DatabaseInteraction.saveTableToDB(queryResult, rowInfo);

				} catch (Exception e) {
					e.printStackTrace();
					System.err.println("Saving Problematic Row: " + rowInfo);
					DatabaseInteraction.saveProblematicSequencesDB(rowInfo);
				}
			}
		} catch (Throwable t) {
			// System.err.println("Exception, could not execute query on
			// database");
			t.printStackTrace();
		} finally {
			// closeConnection();
		}

	}

	public void processProblematicSequences(OptionsOwn opt) {
		System.out.println("Processing Problematic Sequences");
		AccessAreaExtraction extraction = new AccessAreaExtraction();
		try {
			// FIXME check whether this has to be done several times or not,
			// because of disconnections, etc.
			List<RowInfo> relevantRows = DatabaseInteraction.getAllProblematicStatements(opt);
			System.out.println("Number of relevant rows: " + relevantRows.size());

			for (RowInfo rowInfo : relevantRows) {
				try {
//					if (rowInfo.seq != 8667166) {
//						continue;
//					}
					System.out.println("SEQ: " + rowInfo.seq);
					DatabaseInteraction.establishConnection(opt.serverAddress, opt.username, opt.password);
					AccessArea accessArea = extraction.extractAccessArea(rowInfo.statement);
					List<FromItem> fi = accessArea.getFrom();
					// now we have tables in the from clause of the
					// statement
					// for each table we now keyColumn
					Map<String, Table> tables = QueryUtil.getTablesWithKeysFromTheFromItemsOfStatement(fi, opt);

					// for each query from query log
					// perform a query to the DB (SkyServer)
					// internalDB is the DB (internal DB)
					HttpURLConnectionExt internalDB = new HttpURLConnectionExt();
					List<Pair<Table, Object>> queryResult = internalDB.sendGetResultFromQuery(rowInfo,
							(HashMap<String, Table>) tables);
					// store data to our internal DB
					DatabaseInteraction.saveFixedStatementsToDB(queryResult, rowInfo);

				} catch (Exception e) {
					e.printStackTrace();
					System.err.println("Saving Problematic Row: " + rowInfo);
				}
			}
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			// closeConnection();
		}
	}
	
	public void processStrayQueries(OptionsOwn opt) {
		System.out.println("Processing Stray Queries");
		AccessAreaExtraction extraction = new AccessAreaExtraction();
		try {
			// FIXME check whether this has to be done several times or not,
			// because of disconnections, etc.
			Set<RowInfo> relevantRows = DatabaseInteraction.getStrayRowInfos(opt);
			System.out.println("Number of relevant rows: " + relevantRows.size());

			for (RowInfo rowInfo : relevantRows) {
				try {
//					if (rowInfo.seq != 8667166) {
//						continue;
//					}
					System.out.println("SEQ: " + rowInfo.seq);
					DatabaseInteraction.establishConnection(opt.serverAddress, opt.username, opt.password);
					//FIXME check correctness!
					appendSelectPrimaryKeyToRowInfo(rowInfo);
					AccessArea accessArea = extraction.extractAccessArea(rowInfo.statement);
					List<FromItem> fi = accessArea.getFrom();
					
					Map<String, Table> tables = QueryUtil.getTablesWithKeysFromTheFromItemsOfStatement(fi, opt);

					HttpURLConnectionExt internalDB = new HttpURLConnectionExt();
					List<Pair<Table, Object>> queryResult = internalDB.sendGetResultFromQuery(rowInfo,
							(HashMap<String, Table>) tables);
					// store data to our internal DB
					DatabaseInteraction.saveTableToDummyDB(queryResult, rowInfo, "_ROSINA");

				} catch (Exception e) {
					e.printStackTrace();
					System.err.println("Saving Problematic Row: " + rowInfo);
				}
			}
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			// closeConnection();
		}
	}
	
	private void appendSelectPrimaryKeyToRowInfo(RowInfo rowInfo) {
		if (!rowInfo.statement.contains("*")) {
			String fromStatement = rowInfo.fromStatement;
			String[] fromTokens = fromStatement.split(",");
			for (String fromToken : fromTokens) {
				String[] tableTokens = fromToken.split("\\s+");
				String tableName = tableTokens[0];
				String tableAlias = null;
				System.out.println("TableName: " + tableName);
				if (tableTokens.length > 2) {
					if (tableTokens[1].equalsIgnoreCase("as")) {
						tableAlias = tableTokens[2];
					} else {
						throw new IllegalArgumentException("Unexpected table tokens: " + Arrays.toString(tableTokens));
					}
				} else if (tableTokens.length == 2) {
					tableAlias = tableTokens[1];
				}
				String primaryColumnName = DatabaseInteraction.getPrimaryColumnName(tableName);
				if (primaryColumnName != null) {
					String toAdd = primaryColumnName;
					
					if (tableAlias != null) {
						toAdd = tableAlias + "." + toAdd;
					} else {
						toAdd = tableName + "." + toAdd;
					}
					System.out.println("Before: " + rowInfo.statement);
					rowInfo.statement = rowInfo.statement.replace(" from ", ", " + toAdd + " from ");
					System.out.println("After: " + rowInfo.statement);
				}
			}
		}
	}

	public void recommend(OptionsOwn opt) {
		// TODO implement this
	}

	public void evaluate(OptionsOwn opt) {
		// TODO implement this
	}
}
