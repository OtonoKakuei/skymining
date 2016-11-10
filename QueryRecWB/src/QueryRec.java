import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private DatabaseInteraction dbI = new DatabaseInteraction();

	public QueryRec(OptionsOwn opt_) {
		opt = opt_;
		// dbI = new DatabaseInteraction();
		DatabaseInteraction.establishConnection(opt.serverAddress, opt.username, opt.password);
		opt.tablesWithKeys = dbI.getTablesKeys();
	}

	public void preprocess(OptionsOwn opt) {
		// read data piece by piece from the DB
		// parse statements
		// write pre-processed data to the DB

		AccessAreaExtraction extraction = new AccessAreaExtraction();

		try {
			//FIXME check whether this has to be done several times or not, because of disconnections, etc.
			List<RowInfo> relevantRows = dbI.getAllRelevantStatements(opt);
			System.out.println("Number of relevant rows: " + relevantRows.size());
			for (RowInfo rowInfo : relevantRows) {
				if (rowInfo.seq < 272354) {
					continue;
				}
				try {
					System.out.println("SEQ: " + rowInfo.seq);
					DatabaseInteraction.establishConnection(opt.serverAddress, opt.username, opt.password);
					AccessArea accessArea = extraction.extractAccessArea(rowInfo.statement);
					List<FromItem> fi = accessArea.getFrom();
					// now we have tables in the from clause of the
					// statement
					// for each table we now keyColumn
					Map<String, Table> tables = SideClass.getTablesWithKeysFromTheFromItemsOfStatement(fi, opt);
					
					// for each query from query log
					// perform a query to the DB (SkyServer)
					// internalDB is the DB (internal DB)
					HttpURLConnectionExt internalDB = new HttpURLConnectionExt();
					List<Pair<Table, Object>> queryResult = internalDB.sendGetResultFromQuery(rowInfo, (HashMap<String, Table>) tables);
					// store data to our internal DB
					dbI.saveTableToDB(queryResult, rowInfo);
					
				} catch (Exception e) {
					e.printStackTrace();
					System.err.println("Saving Problematic Row: " + rowInfo);
					dbI.saveProblematicSequencesDB(rowInfo);
				}
			}
		} catch (Throwable t) {
//			System.err.println("Exception, could not execute query on database");
			 t.printStackTrace();
		} finally {
			// closeConnection();
		}

	}
	
	public void processProblematicSequences(OptionsOwn opt) {
		System.out.println("Processing Problematic Sequences");
		AccessAreaExtraction extraction = new AccessAreaExtraction();
		try {
			//FIXME check whether this has to be done several times or not, because of disconnections, etc.
			List<RowInfo> relevantRows = dbI.getAllProblematicStatements(opt);
			System.out.println("Number of relevant rows: " + relevantRows.size());

			for (RowInfo rowInfo : relevantRows) {
				if (rowInfo.seq < 272354) {
					continue;
				}
				try {
					System.out.println("SEQ: " + rowInfo.seq);
					DatabaseInteraction.establishConnection(opt.serverAddress, opt.username, opt.password);
					AccessArea accessArea = extraction.extractAccessArea(rowInfo.statement);
					List<FromItem> fi = accessArea.getFrom();
					// now we have tables in the from clause of the
					// statement
					// for each table we now keyColumn
					Map<String, Table> tables = SideClass.getTablesWithKeysFromTheFromItemsOfStatement(fi, opt);
					
					// for each query from query log
					// perform a query to the DB (SkyServer)
					// internalDB is the DB (internal DB)
					HttpURLConnectionExt internalDB = new HttpURLConnectionExt();
					List<Pair<Table, Object>> queryResult = internalDB.sendGetResultFromQuery(rowInfo, (HashMap<String, Table>) tables);
					// store data to our internal DB
					dbI.saveTableToDB(queryResult, rowInfo);
					
				} catch (Exception e) {
					e.printStackTrace();
					System.err.println("Saving Problematic Row: " + rowInfo);
					dbI.saveProblematicSequencesDB(rowInfo);
				}
			}
		} catch (Throwable t) {
//			System.err.println("Exception, could not execute query on database");
			 t.printStackTrace();
		} finally {
			// closeConnection();
		}
	}

	public void recommend(OptionsOwn opt) {
		//TODO implement this
	}

	public void evaluate(OptionsOwn opt) {
		//TODO implement this
	}
}
