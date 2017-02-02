package query.process;

import java.util.Map;

import aima.core.util.datastructure.Pair;
import largespace.business.DatabaseInteraction;
import wb.model.OrderingType;

public class UserSessionSimilarityFunction {

	private static final UserSessionSimilarityFunction INSTANCE = new UserSessionSimilarityFunction();

	private UserSessionSimilarityFunction() {
	}

//	public static Map<Pair<Long, Long>, Float> getSimilarQueries(long userSession, long lastSeq, boolean isOrdered) {
//		if (isOrdered) {
//			return DatabaseInteraction.getSimilarOrderedRecommendations(userSession, lastSeq);
//		} else {
//			return DatabaseInteraction.getSimilarUnorderedRecommendations(userSession, lastSeq, OrderingType.SEQ);
//		}
//	}
}
