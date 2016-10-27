/* Written by FlorianB for SQLQueryLogTransformer:
 * This removes any alias from the relations in the FROM list and sorts them alphabetically using a treemap
 * Also replace aliases in WHERE part by true relation name
 */
package accessarea;

import java.text.Collator;
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
import net.sf.jsqlparser.statement.select.FunctionItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class AliasHandler implements ExpressionVisitor {

	private ExpressionVisitor expressionVisitor;
	private Stack<Expression> stack = new Stack<Expression>();
	private TreeMap<String, String> tm = new TreeMap<String, String>();

	public AccessArea handleAlias(AccessArea accessArea) {
		List<FromItem> from = accessArea.getFrom();
		Expression where = accessArea.getWhere();
		ListIterator<FromItem> it = from.listIterator();
		// Add every relation to the TreeMap, ignore duplicates, remove aliases
		while (it.hasNext()) {
			Object obj = it.next();
			if (obj instanceof Table) {
				Table table = (Table) obj;
				if (table.getAlias() != null)
					tm.put(table.getAlias(), table.getName());
				table.setAlias(null);
			} else {
				FunctionItem table = (FunctionItem) obj;
				if (table.getAlias() != null) {
					tm.put(table.getAlias(), table.getFunction().getName());
					table.setAlias(null);
				}
			}
		}
		// Traverse the expression once more to replace alias by true relation
		// name
		// Only visit column is truly important this time
		if (where != null) {
			where.accept(this);
			where = stack.pop();
		}
		// sort Tables alphabetically
		Collator stringCollator = Collator.getInstance();
		TreeMap<String, FromItem> tm2 = new TreeMap<String, FromItem>(stringCollator);
		ListIterator<FromItem> it2 = from.listIterator();
		while (it2.hasNext()) {
			Object obj = it2.next();
			if (obj instanceof Table) {
				Table table = (Table) obj;
				tm2.put(table.getName(), table);
			} else {
				FunctionItem table = (FunctionItem) obj;
				tm2.put(table.getAlias(), table);

			}
		}
		from = new ArrayList<FromItem>(tm2.values());
		accessArea.setFrom(from);
		accessArea.setWhere(where);
		return accessArea;
	}

	// We look in TreeMap for the alias and replace with true relation name
	public void processColumn(Column tableColumn) {
		if (tm.containsKey(tableColumn.getTable().toString())) {
			tableColumn.setTable(new Table(null, tm.get(tableColumn.getTable().toString())));
		}
		stack.push(tableColumn);
	}

	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		Expression tempLeftExpression = binaryExpression.getLeftExpression();
		Expression tempRightExpression = binaryExpression.getRightExpression();

		tempRightExpression.accept(this);
		tempLeftExpression.accept(this);
		Expression left = stack.pop();
		Expression right = stack.pop();
		Expression parent = stack.pop();
		if (parent != null) {
			if (left == null)
				parent = right;
			else {
				if (left instanceof OrExpression && parent instanceof AndExpression)
					left = new Parenthesis(left);
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
	public void visit(Function function) { // should not occur anymore
		stack.push(function);
	}

	@Override
	public void visit(InverseExpression inverseExpression) { // is a negative
																// value!
		stack.push(inverseExpression);
		inverseExpression.getExpression().accept(this);
		Expression tempExpression = stack.pop();
		inverseExpression = (InverseExpression) stack.pop();
		inverseExpression.setExpression(tempExpression);
		stack.push(inverseExpression);
	}

	@Override
	public void visit(JdbcParameter jdbcParameter) { // should not be necessary
														// here
	}

	@Override
	public void visit(JdbcNamedParameter jdbcNamedParameter) { // should not be
																// necessary
																// here
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
		stack.pop();
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
	public void visit(CaseExpression caseExpression) {// only occurs mostly in
														// select clause in our
														// SDSS share, now
														// information about
														// cases

	}

	@Override
	public void visit(WhenClause whenClause) { // 27 in SDSS: mostly in select
												// part, nearly always as part
												// of case expression

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
	public void visit(ExtractExpression eexpr) { // occurs zero times
	}

	@Override
	public void visit(IntervalExpression iexpr) { // Not in MS SQL?
	}

	@Override
	public void visit(SubSelect subSelect) { // should be handled already
												// somewhere else (EXISTS, IN,
												// ALL, ANY,...) but could occur
												// in WHERE part anyway

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