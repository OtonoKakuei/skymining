package query.process;
import java.util.List;
import java.util.Set;

import com.beust.jcommander.JCommander;

import largespace.business.DatabaseInteraction;
import largespace.business.OptionsOwn;
import wb.model.OrderingType;

public class MainClass {
	private static final MainClass INSTANCE = new MainClass();
	
	private MainClass() {
		
	}
	
	public static void main(String[] args) throws Exception {
		OptionsOwn opt = new OptionsOwn();
		QueryRec rec = new QueryRec(opt);
		new JCommander(opt, args);
		//uncomment if necessary.
//		INSTANCE.doQuerySimilarityFunction(opt);
		INSTANCE.doUserSessionSimilarityFunction(opt);
		
		switch (opt.mode) {
		// preprocessing step store the result of the query to our internal
		// database
		// before the preprocessing step we need to find out which tables we
		// have; which columns they contain; which of them could be identifiers
		// for an object?
		// just a remainder: to compare 2 queries In WB paradigm we need to
		// compare the results of the queries and find common objects.
		// the amount of common objects identify the measure of similarity for 2
		// queries
		// suchwise, first we will get tables with columns (in pre-preprocessing
		// step)
		// after that we create an internal database to store the results of the
		// queries
		// on preprocessing step we fill this internal database

		case 0:
			//pre-processes the sequences that we've stored in the database.
			//uncomment only the necessary rows.
//			rec.preprocess(opt);
//			rec.processProblematicSequences(opt);
//			rec.exportQueryTupleFrequencyToCSV();
//			rec.processStrayQueries(opt);
			break;
		case 1:
			//was not used, because this was done with sql
			rec.recommend(opt);
			break;
		case 2:
			//was not used, because this was done with sql
			rec.evaluate(opt);
			break;
		}

	}
	
	/**
	 * This method was only used to calculate the unordered similarity.
	 * @param opt db conf
	 */
	private void doUserSessionSimilarityFunction(OptionsOwn opt) {
		DatabaseInteraction.establishConnection(opt);
		Long beginTime = System.currentTimeMillis();
		//uncomment rows if necessary.
		
//		Set<SessionInfo> sessionInfos = DatabaseInteraction.getAllSessions();
//		System.err.println("STARTING!!!! after getting sessions" + ((System.currentTimeMillis() - beginTime)) / 1000 + " seconds.");
//		System.out.println("found: " + sessionInfos.size() + " sessions.");
//		DatabaseInteraction.segmentAndInsertUserSessions(sessionInfos);
//		System.err.println("Finished segmenting and inserting user sessions in: " 
//				+ ((System.currentTimeMillis() - beginTime)) / 1000 + " seconds.");
//		beginTime = System.currentTimeMillis();
//		DatabaseInteraction.calculateUnorderedSimilarities(OrderingType.SEQ);
//		System.err.println("SEQ Done");
		DatabaseInteraction.calculateUnorderedSimilarities(OrderingType.TIME);
//		System.err.println("Finished inserting tuples of segmented user sessions in: " 
//				+ ((System.currentTimeMillis() - beginTime)) / 1000 + " seconds.");
		
		//already done
//		DatabaseInteraction.updateExpectedSeq("QRS_US_SIMILARITY");
		
//		DatabaseInteraction.updateExpectedSeq("qrs_ussf_unordered");
//		DatabaseInteraction.updateExpectedSeq("QRS_USSF_FB");
//		DatabaseInteraction.processUnorderedSimilarity(2, OrderingType.SEQ);
		System.err.println("Finished inserting tuples of segmented user sessions in: " 
				+ ((System.currentTimeMillis() - beginTime)) / 1000 + " seconds.");
	}

	/**
	 * Does the query similarity function on all the sequences that can be compared with one another.
	 * This method is replaced by another method in SQL.
	 * @param opt db conf
	 */
	private void doQuerySimilarityFunction(OptionsOwn opt) {
		DatabaseInteraction.establishConnection(opt);
		int i = 1;
		int j = 1;
		List<Long> allComparableSequences = DatabaseInteraction.getAllComparableSequences();
		allComparableSequences.removeAll(DatabaseInteraction.getAllProcessedSimilarSequences());
//		allComparableSequences.removeAll(DatabaseInteraction.getAllSequencesFromQuerySimilarity());
		int size = allComparableSequences.size();
		Long beginTime = System.currentTimeMillis();
		double maxTime = 0.0;
		double totalTime = 0.0;
		double averageTime = 0.0;
		for (Long seq : allComparableSequences) {
			if (j % 500 == 0) {
				DatabaseInteraction.establishConnection(opt.serverAddress, opt.username, opt.password);
			}
			Long beginLoopTime = System.currentTimeMillis();
			System.out.println("Doing seq: " + seq + ", query: " + j + "/" + size);
			Set<Long> similarSequences = QuerySimilarityFunction.getSimilarSequences(seq);
			System.out.print("Obtained similar sequences! ");
			//ONLY NEED TO SAVE THE SEQS ONCE! ONCE THEY'RE STORED IN THE DB, DO NOT STORE THEM AGAIN
			//COMMENT FROM HERE
			DatabaseInteraction.saveSimilarSequences(seq, similarSequences);
			System.out.print("Saved similar sequences! ");
			//COMMENT UNTIL HERE
			double timeNeeded = 0.0;
			if (similarSequences.size() < 100000) {
				DatabaseInteraction.saveQuerySimilarity(seq, QuerySimilarityFunction.getSimilarQueries(seq, similarSequences));
				timeNeeded = ((System.currentTimeMillis() - beginLoopTime)) / 1000.0;
				System.err.println("Query number: " + i + ", Took: " + timeNeeded + " seconds.");
				i++;
			} else {
				timeNeeded = ((System.currentTimeMillis() - beginLoopTime)) / 1000.0;
				System.err.println("Missed, took: " + timeNeeded + " seconds.");
			}
			totalTime += timeNeeded;
			averageTime = totalTime / j;
			if (maxTime < timeNeeded) {
				maxTime = timeNeeded;
			}
			
			System.err.println("TotalTime: " + totalTime + ", AverageTime: " + averageTime + ", MaxTime: " + maxTime);
			
			j++;
		}
		System.out.println("Finished in: " + ((System.currentTimeMillis() - beginTime)) / 1000 + " seconds.");
	}
}
