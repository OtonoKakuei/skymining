/* Written by FlorianB for SQLQueryLogTransformer:
 * After CNF Transformation we have to convert the structure back to our wanted access area with the original predicates
 * The AIMA3e CNF converter returns a list of clauses and literals. so we create as meny ORs and ANDs and parenthesis as necessary
 * and fill each side with the corresponding predicate from the original numbered list of predicates
 */
package accessarea;

import java.util.List;
import java.util.Set;
import java.util.Stack;

import aima.core.logic.fol.kb.data.CNF;
import aima.core.logic.fol.kb.data.Clause;
import aima.core.logic.fol.kb.data.Literal;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;

public class FinalCNF {

	private Stack<Expression> stack = new Stack<Expression>();

	public Expression getCNFPredicates(CNF cnf, List<Expression> expressions) {
		int clauses = cnf.getNumberOfClauses();
		List<Clause> clauseList = cnf.getConjunctionOfClauses();
		if (clauses == 0)
			stack.push(null);
		else if (clauses == 1 && expressions.size() == 1)
			stack.push(expressions.get(0));
		else {
			for (int i = 0; i < clauses; i++) {// For each clause process the
												// literals (i.e. create ORs)
												// and for each clause (if more
												// than one clauses) create an
												// AND
				Clause clause = clauseList.get(i);
				int literals = clause.getNumberLiterals();
				Set<Literal> literalSet = clause.getLiterals();
				Object[] literalArray = literalSet.toArray();
				for (int j = 0; j < literals; j++) {// Create as many ORs as
													// necessary (i.e. count of
													// literals) and fill one
													// side with the current
													// expression
					String literal = literalArray[j].toString();
					int id = Integer.parseInt(literal.substring(3, literal.length() - 1));
					if (j < (literals - 1) && literals != 1) {
						stack.push(new OrExpression(expressions.get(id), null));
					} else
						stack.push(expressions.get(id));
				}
				while (true) { // connect the ORs until stack is empty or an AND
								// is reached
					Expression predicate = stack.pop();
					if (stack.empty()) {
						stack.push(predicate);
						break;
					} else if (stack.peek() instanceof AndExpression) {
						stack.push(predicate);
						break;
					}
					OrExpression orExpression = new OrExpression(null, null);

					orExpression = (OrExpression) stack.pop();

					orExpression.setRightExpression(predicate);
					if (stack.empty()) {
						stack.push(orExpression);
						break;
					} else if (stack.peek() instanceof AndExpression) {
						stack.push(orExpression);
						break;
					}
					stack.push(orExpression);
				}
				Expression parenthesis = new Parenthesis();
				if (literals != 1) {// Create parenthesis if more than 1
									// literals
					parenthesis = new Parenthesis(stack.pop());
				} else {
					parenthesis = stack.pop();
				}
				if (i < clauses - 1 && clauses != 1) {// If more than one clause
														// exists create a new
														// AND
					stack.push(new AndExpression(parenthesis, null));
				} else
					stack.push(parenthesis);
			}
			while (true) {// connect the ANDs until stack is empty
				Expression predicate = stack.pop();
				if (stack.empty()) {
					stack.push(predicate);
					break;
				}
				AndExpression andExpression = new AndExpression(null, null);
				andExpression = (AndExpression) stack.pop();
				andExpression.setRightExpression(predicate);
				if (stack.empty()) {
					stack.push(andExpression);
					break;
				}
				stack.push(andExpression);
			}
		}

		return stack.pop();
	}
}