package query.process;

import java.util.Map;

import aima.core.util.datastructure.Pair;

public class UserSessionSimilarityFunction {

	private static final UserSessionSimilarityFunction INSTANCE = new UserSessionSimilarityFunction();

	private UserSessionSimilarityFunction() {

	}

	public static Map<Pair<Long, Long>, Float> getSimilarQueries(long userSession, long lastSeq, boolean isOrdered) {
		
		return null;
	}
}
