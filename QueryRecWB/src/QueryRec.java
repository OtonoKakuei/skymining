
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

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
	OptionsOwn opt;
	DatabaseInteraction dbI = new DatabaseInteraction();

	public QueryRec(OptionsOwn opt_) {
		opt = opt_;
		// dbI = new DatabaseInteraction();
		DatabaseInteraction.establishConnection(opt.serverAddress, opt.username, opt.password);
		opt.TABLESWITHKEYS = dbI.GetTablesKeys();
	}

	public void Preprocess(OptionsOwn opt) {
		// read data piece by piece from the DB
		// parse statements
		// write pre-processed data to the DB

		AccessAreaExtraction extraction = new AccessAreaExtraction();

		try {

			List<Long> lastSeqList = dbI.GetlastSeq("QRS_LAST_SEQ_WB", "QRS_STATEMENTS_PP");
			Long lastSeq = lastSeqList.get(0);
			Long finalSeq = lastSeqList.get(1);

			Statement st2 = dbI.conn.createStatement();

			Boolean achieveTheFinalSeq = false;

			while (!achieveTheFinalSeq) {
				DatabaseInteraction.establishConnection(opt.serverAddress, opt.username, opt.password); // Also
																										// starts
																										// the
																										// connection

				Long nextSeq = lastSeq + 70;
				// get next N statements;
				List<RowInfo> rows = dbI.getNextNStatements(lastSeq, nextSeq, opt);
				SideClass sc = new SideClass();
				for (RowInfo ri : rows) {
					// there is no point of re-querying if we know that it
					// returns 0 rows
					if (ri.NrRows > 0) {
						AccessArea accessArea = extraction.extractAccessArea(ri.Statement);
						List<FromItem> fi = accessArea.getFrom();
						// now we have tables in the from clause of the
						// statement
						// for each table we now keyColumn
						HashMap<String, Table> tables = sc.GetTablesWithKeysFromTheFromItemsOfStatement(fi, opt);

						// for each query from query log
						// perform a query to the DB (SkyServer)
						// internalDB is the DB (internal DB)
						HttpURLConnectionExt internalDB = new HttpURLConnectionExt();
						List<Pair<Table, Object>> queryResult = internalDB.sendGetResultFromQuery(ri, tables);
						// store data to our internal DB
						dbI.SaveTableToDB(queryResult, ri);
					}
				}
				dbI.SetlastSeq(nextSeq, "QRS_LAST_SEQ_WB");
				lastSeq = nextSeq;
				if (lastSeq >= finalSeq)
					achieveTheFinalSeq = true;
			}

		} catch (Throwable t) {
			System.err.println("Exception, could not execute query on database");
			// t.printStackTrace();
		} finally {
			// closeConnection();
		}

	}

	public void Recommend(OptionsOwn opt) {

	}

	public void Evaluate(OptionsOwn opt) {

	}
}
