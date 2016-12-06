package query.process;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import largespace.business.DatabaseInteraction;
import largespace.business.RowInfo;
import wb.model.TupleInfo;

public class QuerySimilarityFunction {
	private static final QuerySimilarityFunction INSTANCE = new QuerySimilarityFunction();
	private static final Map<Long, Set<TupleInfo>> TUPLE_SEQ_MAP = new HashMap<>();
	
	//should probably comment this one out if memory storage is an issue
	private static final Map<TupleInfo, Set<Long>> TUPLE_SEQS_MAP = new HashMap<>();
	
	private QuerySimilarityFunction() {
		
	}
	
	public static Set<Long> getSimilarSequences(RowInfo rowInfo) {
		return getSimilarSequences(rowInfo.seq);
	}
	
	public static Map<Long, Double> getSimilarQueries(long seq) {
		return getSimilarQueries(seq, getSimilarSequences(seq));
	}
	
	public static Map<Long, Double> getSimilarQueries(long seq, Set<Long> similarSequences) {
		Map<Long, Double> similarQueries = new HashMap<>();
	
		Set<TupleInfo> allTuples = INSTANCE.getAllTuples(seq);
		for (Long similarSeq : similarSequences) {
			similarQueries.put(similarSeq, INSTANCE.calculateSimilarity(allTuples, similarSeq));
		}
		
		return similarQueries;
	}
	
	public static Set<Long> getSimilarSequences(long seq) {
		Set<Long> similarQueries = DatabaseInteraction.getSimilarSequencesFromTable(seq);
		if (similarQueries.isEmpty()) {
			for (TupleInfo tupleInfo : INSTANCE.getAllTuples(seq)) {
				similarQueries.addAll(INSTANCE.getSimilarSequences(tupleInfo));
			}
		}
		return similarQueries;
	}
	
	public static double calculateSimilarity(long seq, long otherSeq) {
		return INSTANCE.calculateSimilarity(INSTANCE.getAllTuples(seq), INSTANCE.getAllTuples(otherSeq));
	}
	
	private double calculateSimilarity(Set<TupleInfo> tupleInfos, long otherSeq) {
		return calculateSimilarity(tupleInfos, getAllTuples(otherSeq));
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
	
	private Set<TupleInfo> getAllTuples(Long seq) {
		Set<TupleInfo> tupleInfos = TUPLE_SEQ_MAP.get(seq);
		if (tupleInfos == null) {
			tupleInfos =  DatabaseInteraction.getAllTuples(seq);
			TUPLE_SEQ_MAP.put(seq, tupleInfos);
		}
		return tupleInfos;
	}
	
	private Set<Long> getSimilarSequences(TupleInfo tupleInfo) {
		Set<Long> similarSequences = TUPLE_SEQS_MAP.get(tupleInfo);
		if (similarSequences == null) {
			similarSequences = DatabaseInteraction.getSimilarSequences(tupleInfo);
			TUPLE_SEQS_MAP.put(tupleInfo, similarSequences);
		}
		return similarSequences;
	}

}
