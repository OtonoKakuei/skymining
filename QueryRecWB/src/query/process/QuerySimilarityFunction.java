package query.process;
import java.util.HashSet;
import java.util.Set;

import largespace.business.DatabaseInteraction;
import largespace.business.RowInfo;
import wb.model.TupleInfo;

public class QuerySimilarityFunction {
	
	public static Set<Long> getSimilarQueries(RowInfo rowInfo) {
		return getSimilarQueries(rowInfo.seq);
	}
	
	public static Set<Long> getSimilarQueries(Long seq) {
		Set<Long> similarQueries = new HashSet<>();
		for (TupleInfo tupleInfo : DatabaseInteraction.getAllTuples(seq)) {
			similarQueries.addAll(DatabaseInteraction.getSimilarSequences(tupleInfo));
		}
		return similarQueries;
	}

}
