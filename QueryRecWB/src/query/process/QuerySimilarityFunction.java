package query.process;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import largespace.business.DatabaseInteraction;
import largespace.business.RowInfo;
import wb.model.TupleInfo;

public class QuerySimilarityFunction {
	private static final QuerySimilarityFunction INSTANCE = new QuerySimilarityFunction();
	
	private QuerySimilarityFunction() {
		
	}
	
	public static Set<Long> getSimilarSequences(RowInfo rowInfo) {
		return getSimilarSequences(rowInfo.seq);
	}
	
	public static Map<Long, Double> getSimilarQueries(long seq) {
		Map<Long, Double> similarQueries = new HashMap<>();
	
		Set<TupleInfo> allTuples = DatabaseInteraction.getAllTuples(seq);
		for (Long similarSeq : getSimilarSequences(seq)) {
			similarQueries.put(similarSeq, INSTANCE.calculateSimilarity(allTuples, similarSeq));
		}
		
		return similarQueries;
	}
	
	public static Set<Long> getSimilarSequences(long seq) {
		Set<Long> similarQueries = new HashSet<>();
		for (TupleInfo tupleInfo : DatabaseInteraction.getAllTuples(seq)) {
			similarQueries.addAll(DatabaseInteraction.getSimilarSequences(tupleInfo));
		}
		return similarQueries;
	}
	
	public static double calculateSimilarity(long seq, long otherSeq) {
		return INSTANCE.calculateSimilarity(DatabaseInteraction.getAllTuples(seq), DatabaseInteraction.getAllTuples(otherSeq));
	}
	
	private double calculateSimilarity(Set<TupleInfo> tupleInfos, long otherSeq) {
		return calculateSimilarity(tupleInfos, DatabaseInteraction.getAllTuples(otherSeq));
	}
	
	private double calculateSimilarity(Set<TupleInfo> tupleInfos, Set<TupleInfo> otherTupleInfos) {
		//FIXME primitive similarity method
		double numberOfFirstTuples = tupleInfos.size();
		double numberOfSecondTuples = otherTupleInfos.size();
		double sameTupleCount = 0.0;
		for (TupleInfo tupleInfo : tupleInfos) {
			if (otherTupleInfos.contains(tupleInfo)) {
				sameTupleCount++;
			}
		}
		
		return sameTupleCount / (numberOfFirstTuples + numberOfSecondTuples - sameTupleCount);
	}

}
