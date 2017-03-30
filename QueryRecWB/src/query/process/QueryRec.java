package query.process;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

	public QueryRec(OptionsOwn opt) {
		this.opt = opt;
		// dbI = new DatabaseInteraction();
		DatabaseInteraction.establishConnection(opt);
		this.opt.tablesWithKeys = DatabaseInteraction.getTablesKeys();
	}

	public void preprocess(OptionsOwn opt) {
		// read data piece by piece from the DB
		// parse statements
		// write pre-processed data to the DB

		AccessAreaExtraction extraction = new AccessAreaExtraction();

		try {
			List<RowInfo> relevantRows = DatabaseInteraction.getAllRelevantStatements(opt);
			System.out.println("Number of relevant rows: " + relevantRows.size());
			for (RowInfo rowInfo : relevantRows) {
				try {
					System.out.println("SEQ: " + rowInfo.seq);
					DatabaseInteraction.establishConnection(opt);
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
					DatabaseInteraction.saveTableToDBWithNoDuplicates(queryResult, rowInfo);

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
			List<RowInfo> relevantRows = DatabaseInteraction.getAllProblematicStatements(opt);
			System.out.println("Number of relevant rows: " + relevantRows.size());

			for (RowInfo rowInfo : relevantRows) {
				try {
					System.out.println("SEQ: " + rowInfo.seq);
					DatabaseInteraction.establishConnection(opt);
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
			Set<RowInfo> relevantRows = DatabaseInteraction.getStrayRowInfos(opt);
			
			int relevantRowsSize = relevantRows.size();
			System.out.println("Number of relevant rows: " + relevantRowsSize);
			int i = 0;
			for (RowInfo rowInfo : relevantRows) {
				try {
					i++;
					System.out.println("Processing SEQ: " + rowInfo.seq + ", progress: " + i + "/" + relevantRowsSize);
					DatabaseInteraction.establishConnection(opt);
					//There was a problem we found with some tuples.
					//Some of the tuples didn't return a primary key, so it was impossible to decide whether
					//they belong to the numeric or string tuple table. Sequences that return such tuples won't be processed, and thus
					//considered as one of the "stray" sequences.
					//Which is why this method is used to append a select clause statement of the primary column name.
					appendSelectPrimaryKeyToRowInfo(rowInfo);
					AccessArea accessArea = extraction.extractAccessArea(rowInfo.statement);
					List<FromItem> fi = accessArea.getFrom();
					
					Map<String, Table> tables = QueryUtil.getTablesWithKeysFromTheFromItemsOfStatement(fi, opt);

					HttpURLConnectionExt internalDB = new HttpURLConnectionExt();
					List<Pair<Table, Object>> queryResult = internalDB.sendGetResultFromQuery(rowInfo,
							(HashMap<String, Table>) tables);
					// store data to our internal DB
					System.out.println("Saving to dummy");
					DatabaseInteraction.saveTableToDB(queryResult, rowInfo);

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
	
	/**
	 * As the name implies. This method appends a select clause statement(s) of the primary column(s) from
	 * tables mentioned in the "from" clause.  
	 * @param rowInfo the given {@link RowInfo}
	 */
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
					rowInfo.statement = rowInfo.statement.replace(" from ", ", " + toAdd + " from ");
				}
			}
		}
	}
	
	/**
	 * Exports the tuple information to CSV in order to plot the data into a graph in excel/spreadsheet.
	 */
	public void exportQueryTupleFrequencyToCSV() {
		String filename = "Test3.csv";
		DatabaseInteraction.establishConnection(opt);
		StringBuilder sb = new StringBuilder("");
		System.out.println("Starting");
		int i = 0;
		List<Pair<Long, Integer>> queryTupleFrequencyPairs = DatabaseInteraction.getTupleFrequency();
		for (Pair<Long, Integer> pair : queryTupleFrequencyPairs) {
			System.out.println("processing: " + i++ + "/" + queryTupleFrequencyPairs.size());
			String seqFrequency = pair.getFirst() + "," + pair.getSecond() + "\n";
			System.out.print(seqFrequency);
			sb.append(seqFrequency);
		}
		System.out.println("Done");
		try {
			File file = new File(filename);
			System.out.println("AbsolutePath: " + file.getAbsolutePath());
			FileWriter writer = new FileWriter(file);
			writer.write(sb.toString());
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void recommend(OptionsOwn opt) {
		// TODO implement this
	}

	public void evaluate(OptionsOwn opt) {
		// TODO implement this
	}
}
