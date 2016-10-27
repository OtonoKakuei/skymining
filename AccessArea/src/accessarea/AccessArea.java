/* Written by FlorianB for SQLQueryLogTransformer:
 * This class defines our new Object type, the AccessArea, with a FROM part and a WHERE part
 * These are a List of FromItems, which are in the end always names of relations
 * and an Expression, if applicable consisting of more expressions (AND, OR, etc.)
 * Includes getters and setters
 */
package accessarea;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;

public final class AccessArea {
	private List<FromItem> from;
	private Expression where;

	public AccessArea(List<FromItem> from, Expression where) {
		this.from = from;
		this.where = where;
	}

	public List<FromItem> getFrom() {
		return from;
	}

	public Expression getWhere() {
		return where;
	}

	public void setFrom(List<FromItem> from) {
		this.from = from;
	}

	public void setWhere(Expression where) {
		this.where = where;
	}

	@Override
	public String toString() {
		return from.toString() + ", " + where;
	}
}
