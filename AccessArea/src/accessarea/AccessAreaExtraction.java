/* Written by FlorianB for SQLQueryLogTransformer:
 * This is the starting point of AccessArea extraction.
 * A SQL query is the only parameter for the main function of this class
 * It will be cleaned first of typical SDSS String errors
 * and then passed through the single steps of extraction.
 * These are Parsing and Transformation, CNF Preparation, CNF transformation,
 * Final CNF construct, Self-Join-Handling and Consolidation and Cleaning
 */
package accessarea;

import java.util.ArrayList;
import aima.core.logic.fol.CNFConverter;
import aima.core.logic.fol.domain.FOLDomain;
import aima.core.logic.fol.kb.data.CNF;
import aima.core.logic.fol.parsing.FOLParser;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.statement.select.FromItem;

public class AccessAreaExtraction {

	static AccessArea accessArea;
	static SQLParser parser = new SQLParser();

	/**
	 * @param args
	 * @throws JSQLParserException
	 */

	public AccessArea extractAccessAreaWithoutUDFs(String stmt) {
		stmt = stmt.replaceAll("\\'\\<.*\\>'", "1");
		// The following cleans the SQL statement from any typical SDSS String
		// errors
		stmt = correctSDSSLogErrors(stmt); // Remove this line if used on DB
											// other than SDSS and if the
											// corrections could clash with log
											// contents

		// Parse the Query with the jSQL parser as a SELECT statement and
		// extract the relations and predicates (calls the Converter class)
		try {
			// long startTime = new Date().getTime();
			accessArea = parser.parseStmtWithoutUDFs(stmt);
			// System.out.println("Transform: " + ((new
			// Date().getTime())-startTime) + " milliseconds");
		} catch (JSQLParserException e) {
			Expression expression = new NullValue();
			accessArea = new AccessArea(new ArrayList<FromItem>(), expression);
			// e.printStackTrace();
			return accessArea;
		}

		// Pass the access area through the subsequent steps: CNF preparation,
		// conversion, self-join-handling and consolidation
		try {
			if (accessArea.getWhere() != null) {
				if (!(accessArea.getWhere() instanceof NullValue || accessArea.getWhere() instanceof BooleanValue)) {
					FOLDomain folDomain = new FOLDomain();
					folDomain.addPredicate("P"); // Our own syntax for cnf
													// conversion
					FOLParser folParser = new FOLParser(folDomain);
					CNFConverter cnfConverter = new CNFConverter(folParser);

					// Prepare our predicates for CNF conversion
					CNFPreparation cnfPreparation = new CNFPreparation();
					PreparedPredicates preparedAccessArea = cnfPreparation.prepareForCNF(accessArea.getWhere());

					if (preparedAccessArea == null)
						return null; // Something went wrong, usually does not
										// happen anymore

					// if(preparedAccessArea.getWhere().toString().contains("remove(p"))
					// return null; //Was used when limiting to 35 predicates,
					// did not always work as expected

					CNF cnf = null;
					// Convert to CNF using the AIMA3e CNF Converter
					try {
						cnf = cnfConverter.convertToCNF(folParser.parse(preparedAccessArea.getWhere().toString()));
					} catch (Throwable t) {
					}

					FinalCNF finalCNF = new FinalCNF();
					// Construct our final CNF
					Integer expressionsCount = preparedAccessArea.getExpressions().size();
					if (cnf != null)
						accessArea.setWhere(finalCNF.getCNFPredicates(cnf, preparedAccessArea.getExpressions()));

					// Self-Join Handler:
					// SelfJoinHandler selfJoinHandler = new SelfJoinHandler();
					// accessArea = selfJoinHandler.handleSelfJoins(accessArea);

					// Consolidation:
					if (expressionsCount <= 10) {
						Consolidation consolidation = new Consolidation();
						accessArea.setWhere(consolidation.consolidate(accessArea.getWhere()));
					} else {
						expressionsCount = expressionsCount * 1;
					}

				}
			}
		} catch (NullPointerException e) {
			// nothing to do, will return null in this case
		}
		try { // If there are relations we remove any alias and sort them
				// alphabetically
			if (!accessArea.getFrom().isEmpty()) {
				AliasHandler aliasHandler = new AliasHandler();
				accessArea = aliasHandler.handleAlias(accessArea);
			}
		} catch (NullPointerException e) {
			// nothing to do, will return null in this case
		}
		return accessArea;
	}

	public AccessArea extractAccessArea(String stmt) {
		stmt = stmt.replaceAll("\\'\\<.*\\>'", "1");
		// The following cleans the SQL statement from any typical SDSS String
		// errors
		stmt = correctSDSSLogErrors(stmt); // Remove this line if used on DB
											// other than SDSS and if the
											// corrections could clash with log
											// contents

		// Parse the Query with the jSQL parser as a SELECT statement and
		// extract the relations and predicates (calls the Converter class)
		try {
			// long startTime = new Date().getTime();
			accessArea = parser.parseStmt(stmt);
			// System.out.println("Transform: " + ((new
			// Date().getTime())-startTime) + " milliseconds");
		} catch (JSQLParserException e) {
			Expression expression = new NullValue();
			accessArea = new AccessArea(new ArrayList<FromItem>(), expression);
			// e.printStackTrace();
			return accessArea;
		}

		// Pass the access area through the subsequent steps: CNF preparation,
		// conversion, self-join-handling and consolidation
		try {
			if (accessArea.getWhere() != null) {
				if (!(accessArea.getWhere() instanceof NullValue || accessArea.getWhere() instanceof BooleanValue)) {
					FOLDomain folDomain = new FOLDomain();
					folDomain.addPredicate("P"); // Our own syntax for cnf
													// conversion
					FOLParser folParser = new FOLParser(folDomain);
					CNFConverter cnfConverter = new CNFConverter(folParser);

					// Prepare our predicates for CNF conversion
					CNFPreparation cnfPreparation = new CNFPreparation();
					PreparedPredicates preparedAccessArea = cnfPreparation.prepareForCNF(accessArea.getWhere());

					if (preparedAccessArea == null)
						return null; // Something went wrong, usually does not
										// happen anymore

					// if(preparedAccessArea.getWhere().toString().contains("remove(p"))
					// return null; //Was used when limiting to 35 predicates,
					// did not always work as expected

					CNF cnf = null;
					// Convert to CNF using the AIMA3e CNF Converter
					try {
						cnf = cnfConverter.convertToCNF(folParser.parse(preparedAccessArea.getWhere().toString()));
					} catch (Throwable t) {
					}

					FinalCNF finalCNF = new FinalCNF();
					// Construct our final CNF
					Integer expressionsCount = preparedAccessArea.getExpressions().size();
					if (cnf != null)
						accessArea.setWhere(finalCNF.getCNFPredicates(cnf, preparedAccessArea.getExpressions()));

					// Self-Join Handler:
					// SelfJoinHandler selfJoinHandler = new SelfJoinHandler();
					// accessArea = selfJoinHandler.handleSelfJoins(accessArea);

					// Consolidation:
					if (expressionsCount <= 10) {
						Consolidation consolidation = new Consolidation();
						accessArea.setWhere(consolidation.consolidate(accessArea.getWhere()));
					} else {
						expressionsCount = expressionsCount * 1;
					}

				}
			}
		} catch (NullPointerException e) {
			// nothing to do, will return null in this case
		}
		try { // If there are relations we remove any alias and sort them
				// alphabetically
			if (!accessArea.getFrom().isEmpty()) {
				AliasHandler aliasHandler = new AliasHandler();
				accessArea = aliasHandler.handleAlias(accessArea);
			}
		} catch (NullPointerException e) {
			// nothing to do, will return null in this case
		}
		return accessArea;
	}

	// Cleaning of typical SDSS String errors from the CSV files
	private String correctSDSSLogErrors(String stmt) {

		// if(stmt.toLowerCase().startsWith("set parseonly on")){//solved
		// further below
		// stmt = stmt.substring(16);
		// }
		if (stmt.contains("..")) {
			stmt = stmt.replace("..", ".");
		}
		if (stmt.contains("&#8805;")) {
			stmt = stmt.replace("&#8805;", ">=");
		}
		if (stmt.contains("into ") && !stmt.contains(" into")) {
			stmt = stmt.replace("into", " into");
		}
		if (stmt.contains("INTO ") && !stmt.contains(" INTO")) {
			stmt = stmt.replace("INTO", " INTO");
		}
		if (stmt.contains("&#8772;")) {
			stmt = stmt.replace("&#8772;", ">=");
		}
		if (stmt.contains("&#8722;")) {
			stmt = stmt.replace("&#8722;", ">=");
		}
		if (stmt.contains("&#8318;")) {
			stmt = stmt.replace("&#8318;", ">=");
		}
		if (stmt.contains("&#8232;")) {
			stmt = stmt.replace("&#8232;", ">=");
		}
		if (stmt.contains("&#65292;")) {
			stmt = stmt.replace("&#65292;", ">=");
		}
		if (stmt.contains("&#65306;")) {
			stmt = stmt.replace("&#65306;", ">=");
		}
		if (stmt.contains("&#65289;")) {
			stmt = stmt.replace("&#65289;", ">=");
		}
		if (stmt.contains("&#61605;")) {
			stmt = stmt.replace("&#61605;", ">=");
		}
		if (stmt.contains("[br]")) {
			stmt = stmt.replace("[br]", " ");
		}
		stmt = stmt.replace("select", " select ");
		stmt = stmt.replace("SELECT", " SELECT ");
		stmt = stmt.replace("Select", " Select ");
		stmt = stmt.replace("from ", " from "); // Functions do not end with
												// from
		stmt = stmt.replace("FROM ", " FROM "); // only < 8000 where FROM occurs
												// two times -> subqueries or
												// comments, not used inside
												// special names like functions
		stmt = stmt.replace("From ", " From "); // Functions do not end with
												// from
		stmt = stmt.replace("where ", " where ");
		stmt = stmt.replace("WHERE ", " WHERE ");
		stmt = stmt.replace("Where ", " Where ");
		stmt = stmt.substring(stmt.toLowerCase().indexOf("select"), stmt.length());
		return stmt;
	}

}
