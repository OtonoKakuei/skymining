/* Written by FlorianB for SQLQueryLogTransformer:
 * Checks the list of relations for multiple occurrences and handles it as described in the thesis
 */
package accessarea;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Stack;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SelfJoinHandler implements ExpressionVisitor {

	private ExpressionVisitor expressionVisitor;
	private Stack<Expression> stack = new Stack<Expression>();
	private boolean firstRun = true;
	private boolean foundAlias = true;
	private String alias1 = "";
	private String alias2 = "";
	private TreeMap<String, Expression> tmPred = new TreeMap<String, Expression>();

	public AccessArea handleSelfJoins(AccessArea accessArea) {
		List<FromItem> from = accessArea.getFrom();
		Expression where = accessArea.getWhere();
		ListIterator<FromItem> it = from.listIterator();
		List<String> selfJoinS = new ArrayList<String>();
		// Iterate through list of relations, add to TreeMap, see if duplicate
		// (don't add in that case)
		// If duplicate was found, then remove one occurrence, go through the
		// constraints, search for its alias and replace on every column with
		// name of the relation
		while (it.hasNext()) {
			Table table = (Table) it.next();
			alias1 = table.getName(); // This is the relation name
			firstRun = true;
			alias2 = alias1;
			if (tmPred.containsKey(alias1)) {
				it.remove();
				if (!selfJoinS.contains(alias1))
					selfJoinS.add(alias1);
			} else {
				where.accept(this);
				where = stack.pop();
			}
			alias2 = table.getAlias(); // This is the alias
			if (alias2 != null) {
				where.accept(this);
				where = stack.pop();
			}
		}
		firstRun = false; // now we have to go through the epxression again and
							// whenever a table that was part of a self join is
							// addressed, we replace it with our new expression
							// of self-join predicates
		ListIterator<FromItem> it2 = from.listIterator();
		while (it2.hasNext()) {
			foundAlias = false;
			Table table = (Table) it2.next();
			alias1 = table.getName();
			if (selfJoinS.contains(table.getName())) {
				where.accept(this);
				where = stack.pop();
			}
		}
		accessArea.setFrom(from);
		accessArea.setWhere(where);
		return accessArea;
	}

	// Whenever we reach a column we have to check to which table and or alias
	// it belongs, replace the alias if its part of a self join and store the
	// predicate
	public void processColumn(Column tableColumn) {
		Expression expression = tableColumn;
		String column = tableColumn.getTable().toString();
		if (firstRun) {// replace alias on any column by name of relation (if it
						// is part of a self-join)
			if (column.equals(alias2) && !alias1.equals(alias2)) {
				tableColumn.setTable(new Table(null, alias1));
			}
			if (column.equals(alias2)) {
				Expression right = stack.pop();
				Expression parent = null;
				if (right instanceof IsNullExpression) {
					parent = right;
					((IsNullExpression) parent).setLeftExpression(tableColumn);
				} else {
					parent = stack.peek();
					((BinaryExpression) parent).setLeftExpression(tableColumn);
					((BinaryExpression) parent).setRightExpression(right);
				}
				stack.push(right);
				Expression tempExpression = parent;
				if (!tmPred.containsKey(alias1))
					tmPred.put(alias1, tempExpression);
				else {
					Expression currentExpression = tmPred.get(alias1);
					if (!currentExpression.toString().contains(tempExpression.toString())) {
						OrExpression selfJoinPredicates = new OrExpression(tmPred.get(alias1), tempExpression);
						tmPred.put(alias1, selfJoinPredicates);
					}
				}
				expression = tableColumn;
			}
		} else if (column.equals(alias1)) {// in second run we replace any
											// self-join expression with our new
											// set of predicates and in any
											// clause we do this only once. all
											// other self-join predicates are
											// removed from the clause
			if (!(stack.pop() instanceof IsNullExpression)) {
				stack.pop();
				stack.push(null);
			}
			stack.push(null);
			if (foundAlias) {
				expression = null;
			} else {
				expression = tmPred.get(column);
				foundAlias = true;
			}
		}
		stack.push(expression);
	}

	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		Expression tempLeftExpression = binaryExpression.getLeftExpression();
		Expression tempRightExpression = binaryExpression.getRightExpression();
		if (stack.peek() instanceof AndExpression && foundAlias)
			foundAlias = false;// Start fresh for every new clause

		// Only traverse ANDs, ORs or direct children. we do not need to get
		// deeper into the hierarchy (e.g., no formulas)
		if (tempRightExpression instanceof BinaryExpression
				&& (stack.peek() instanceof AndExpression || stack.peek() instanceof OrExpression))
			tempRightExpression.accept(this);
		else if (tempRightExpression instanceof Column) {
			if (((Column) tempRightExpression).getTable().toString().equals(alias1)
					|| ((Column) tempRightExpression).getTable().toString().equals(alias2))
				((Column) tempRightExpression).setTable(new Table(null, alias1));
			stack.push(tempRightExpression);
		} else
			stack.push(tempRightExpression);
		tempLeftExpression.accept(this);
		Expression left = stack.pop();
		Expression right = stack.pop();
		Expression parent = stack.pop();
		if (parent != null) {// If any predicate was deleted handle it
								// accordingly, make a nice correct structure
			if (left == null)
				parent = right;
			else {
				if (left instanceof OrExpression && parent instanceof AndExpression)
					left = new Parenthesis(left);
				if (right instanceof OrExpression && parent instanceof AndExpression)
					right = new Parenthesis(right);
				((BinaryExpression) parent).setLeftExpression(left);
				((BinaryExpression) parent).setRightExpression(right);
			}
		} else
			parent = left;
		stack.push(parent);
	}

	@Override
	public void visit(NullValue nullValue) {
		stack.push(nullValue);
	}

	@Override
	public void visit(Function function) {
		stack.push(function);
	}

	@Override
	public void visit(InverseExpression inverseExpression) { // is a negative
																// value
		stack.push(inverseExpression);
		inverseExpression.getExpression().accept(this);
		Expression tempExpression = stack.pop();
		inverseExpression = (InverseExpression) stack.pop();
		inverseExpression.setExpression(tempExpression);
		stack.push(inverseExpression);
	}

	@Override
	public void visit(JdbcParameter jdbcParameter) {
	}

	@Override
	public void visit(JdbcNamedParameter jdbcNamedParameter) {
	}

	@Override
	public void visit(DoubleValue doubleValue) {
		stack.push(doubleValue);
	}

	@Override
	public void visit(LongValue longValue) {
		stack.push(longValue);
	}

	@Override
	public void visit(HexValue hexValue) {
		stack.push(hexValue);
	}

	@Override
	public void visit(DateValue dateValue) {
		stack.push(dateValue);
	}

	@Override
	public void visit(TimeValue timeValue) {
		stack.push(timeValue);
	}

	@Override
	public void visit(TimestampValue timestampValue) {
		stack.push(timestampValue);
	}

	@Override
	public void visit(Parenthesis parenthesis) {
		stack.push(parenthesis);
		foundAlias = false;
		Expression tempExpression = parenthesis.getExpression();
		tempExpression.accept(this);
		tempExpression = stack.pop();
		parenthesis = (Parenthesis) stack.pop();
		parenthesis.setExpression(tempExpression);
		stack.push(parenthesis);
	}

	@Override
	public void visit(StringValue stringValue) {
		stack.push(stringValue);
	}

	@Override
	public void visit(Addition addition) {
		stack.push(new Addition());
		visitBinaryExpression(addition);
	}

	@Override
	public void visit(Division division) {
		stack.push(new Division());
		visitBinaryExpression(division);
	}

	@Override
	public void visit(Multiplication multiplication) {
		stack.push(new Multiplication());
		visitBinaryExpression(multiplication);
	}

	@Override
	public void visit(Subtraction subtraction) {
		stack.push(new Subtraction());
		visitBinaryExpression(subtraction);
	}

	@Override
	public void visit(AndExpression andExpression) {
		stack.push(new AndExpression(null, null));
		visitBinaryExpression(andExpression);
	}

	@Override
	public void visit(OrExpression orExpression) {
		stack.push(new OrExpression(null, null));
		visitBinaryExpression(orExpression);
	}

	@Override
	public void visit(Between between) {
		stack.push(between);
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		stack.push(new EqualsTo());
		visitBinaryExpression(equalsTo);
	}

	@Override
	public void visit(GreaterThan greaterThan) {
		stack.push(new GreaterThan());
		visitBinaryExpression(greaterThan);
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		stack.push(new GreaterThanEquals());
		visitBinaryExpression(greaterThanEquals);
	}

	@Override
	public void visit(InExpression inExpression) { // two types, list or
													// subquery, is list in sdss
													// server often?

	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
		stack.push(isNullExpression);
		processColumn((Column) isNullExpression.getLeftExpression());
		isNullExpression.setLeftExpression(stack.pop());
		if (stack.pop() == null)
			isNullExpression = null;
		stack.push(isNullExpression);
	}

	@Override
	public void visit(LikeExpression likeExpression) {
		stack.push(new LikeExpression());
		visitBinaryExpression(likeExpression);
	}

	@Override
	public void visit(MinorThan minorThan) {
		stack.push(new MinorThan());
		visitBinaryExpression(minorThan);
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		stack.push(new MinorThanEquals());
		visitBinaryExpression(minorThanEquals);
	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		stack.push(new NotEqualsTo());
		visitBinaryExpression(notEqualsTo);
	}

	@Override
	public void visit(Column tableColumn) {
		processColumn(tableColumn);
	}

	@Override
	public void visit(CaseExpression caseExpression) {

	}

	@Override
	public void visit(WhenClause whenClause) {

	}

	@Override
	public void visit(ExistsExpression existsExpression) {

	}

	@Override
	public void visit(AllComparisonExpression allComparisonExpression) {

	}

	@Override
	public void visit(AnyComparisonExpression anyComparisonExpression) {

	}

	@Override
	public void visit(Concat concat) {
		stack.push(new Concat());
		visitBinaryExpression(concat);
	}

	@Override
	public void visit(Matches matches) {
	}

	@Override
	public void visit(BitwiseAnd bitwiseAnd) {
		stack.push(new BitwiseAnd());
		visitBinaryExpression(bitwiseAnd);
	}

	@Override
	public void visit(BitwiseOr bitwiseOr) {
		stack.push(new BitwiseOr());
		visitBinaryExpression(bitwiseOr);
	}

	@Override
	public void visit(BitwiseXor bitwiseXor) {
		stack.push(new BitwiseXor());
		visitBinaryExpression(bitwiseXor);
	}

	@Override
	public void visit(CastExpression cast) {
	}

	@Override
	public void visit(Modulo modulo) {
		stack.push(new Modulo());
		visitBinaryExpression(modulo);
	}

	@Override
	public void visit(AnalyticExpression aexpr) {
	}

	@Override
	public void visit(ExtractExpression eexpr) {
	}

	@Override
	public void visit(IntervalExpression iexpr) {
	}

	@Override
	public void visit(SubSelect subSelect) {

	}

	public ExpressionVisitor getExpressionVisitor() {
		return expressionVisitor;
	}

	public void setExpressionVisitor(ExpressionVisitor visitor) {
		expressionVisitor = visitor;
	}

	@Override
	public void visit(BooleanValue booleanValue) {
		stack.push(booleanValue);
	}
}