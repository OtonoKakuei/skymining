package query.process;
import java.util.HashSet;
import java.util.Set;

import javafx.util.Pair;
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
	
	public static Set<Pair<Long, Double>> getSimilarQueries(long seq) {
		Set<Pair<Long, Double>> similarQueries = new HashSet<>();
	
		Set<TupleInfo> allTuples = DatabaseInteraction.getAllTuples(seq);
		for (Long similarSeq : getSimilarSequences(seq)) {
			similarQueries.add(new Pair<>(similarSeq, INSTANCE.calculateSimilarity(allTuples, similarSeq)));
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
