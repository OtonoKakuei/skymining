/* Written by FlorianB for SQLQueryLogTransformer:
 * Second new Object type for list of expressions, which represent the original predicates from our access area
 * and the new where expression which is in the structure necessary for CNF conversion
 * Including getters and setters
 */
package accessarea;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;

final class PreparedPredicates {
	private List<Expression> expressions;
	private Expression where;

	public PreparedPredicates(List<Expression> expressions, Expression where) {
		this.expressions = expressions;
		this.where = where;
	}

	public List<Expression> getExpressions() {
		return expressions;
	}

	public Expression getWhere() {
		return where;
	}

	public void setExpressions(List<Expression> expressions) {
		this.expressions = expressions;
	}

	public void setWhere(Expression where) {
		this.where = where;
	}

	@Override
	public String toString() {
		return expressions.toString() + ", " + where;
	}
}
