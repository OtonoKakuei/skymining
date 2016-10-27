/* Written by FlorianB for SQLQueryLogTransformer:
 * Since the current CNF converter form the AIMA3e Project works on its own structure we convert our access area
 */
package accessarea;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

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
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
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
import net.sf.jsqlparser.statement.select.SubSelect;

public class CNFPreparation implements ExpressionVisitor {

	private ExpressionVisitor expressionVisitor;
	private Stack<Expression> stack = new Stack<Expression>();
	private NullValue initExpression = new NullValue();
	private PreparedPredicates preparedPredicates = new PreparedPredicates(new ArrayList<Expression>(), initExpression);
	private ArrayList<Expression> predicateList = new ArrayList<Expression>();;
	private int predicateCounter = 0;
	private Boolean tooMany = false;

	// We traverse the constraint once, store every predicate in a numbered list
	// and replace it in the where expression by P(px), e.g. P(p1)
	public PreparedPredicates prepareForCNF(Expression predicate) {
		predicate.accept(this);
		Expression where = stack.pop();

		// AIMA3e CNF converter always needs surrounding parenthesis
		if (!(where instanceof Parenthesis)) {
			Parenthesis tempParenthesis = new Parenthesis();
			tempParenthesis.setExpression(where);
			where = tempParenthesis;
		}
		// for constructing our final CNF constraints we need the list of
		// original predicates and the new expression with P(px)
		preparedPredicates.setExpressions(predicateList);

		// Currently CNF Converter get's stuck with more than 35 predicates in
		// one access area
		// If you want to use our method of limiting any larger access area to
		// the first 35 predicates remove the following line
		if (predicateCounter > 35)
			return null;

		// the following while loop searches the predicate with the currently
		// highest number and removes it.
		// This is done until only 35 predicates are left. The number can be
		// changed of course, only a parameter
		while (predicateCounter > 35) {
			stack.clear();
			tooMany = true;
			where.accept(this);
		}
		preparedPredicates.setWhere(where);
		return preparedPredicates;
	}

	// Here we replace predicates with P(px), save the original predicate in our
	// list of expressions and increment the counter
	public void processExpression(Expression expression) {
		Function cnfPredicate = new Function();
		cnfPredicate.setName("P");
		Column cnfColumn = new Column(new Table(null, null), "p" + predicateCounter);
		List<Expression> cnfExpressions = new ArrayList<Expression>();
		cnfExpressions.add(cnfColumn);
		cnfPredicate.setParameters(new ExpressionList(cnfExpressions));
		predicateList.add(expression);
		++predicateCounter;
		stack.push(cnfPredicate);
	}

	// in normal mode we only traverse the right hand side and left hand side of
	// each binary expression
	// if we are in "tooMany" mode, i.e. more than 35 (or whatever) predicates
	// are in the current access area, then we catch the one with the largest
	// number and remove it.
	// this is done here since we cannot change the structure inside the visit
	// methods
	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		Expression tempLeftExpression = binaryExpression.getLeftExpression();
		Expression tempRightExpression = binaryExpression.getRightExpression();

		tempLeftExpression.accept(this);
		tempRightExpression.accept(this);

		if (tooMany) {// Dont care about parenthesis, are only searching for
						// predicate with highest number (i.e. it's name is now
						// "remove(px)"
			if (tempLeftExpression instanceof Parenthesis) {
				tempLeftExpression = ((Parenthesis) tempLeftExpression).getExpression();
			}
			if (tempRightExpression instanceof Parenthesis) {
				tempRightExpression = ((Parenthesis) tempRightExpression).getExpression();
			}
			if (tempLeftExpression instanceof BinaryExpression) {
				Expression tempLeftLeftExpression = ((BinaryExpression) tempLeftExpression).getLeftExpression();
				Expression tempLeftRightExpression = ((BinaryExpression) tempLeftExpression).getRightExpression();
				if (tempLeftLeftExpression instanceof Parenthesis)
					tempLeftLeftExpression = ((Parenthesis) tempLeftLeftExpression).getExpression();
				if (tempLeftRightExpression instanceof Parenthesis)
					tempLeftRightExpression = ((Parenthesis) tempLeftRightExpression).getExpression();
				if (tempLeftLeftExpression instanceof Function) {
					if ((((Function) tempLeftLeftExpression).getName().equals("remove"))) {
						binaryExpression.setLeftExpression(tempLeftRightExpression);
					}
				} else if (tempLeftRightExpression instanceof Function) {
					if ((((Function) tempLeftRightExpression).getName().equals("remove"))) {
						binaryExpression.setLeftExpression(tempLeftLeftExpression);
					}
				}
			}
			if (tempRightExpression instanceof BinaryExpression) {
				Expression tempRightLeftExpression = ((BinaryExpression) tempRightExpression).getLeftExpression();
				Expression tempRightRightExpression = ((BinaryExpression) tempRightExpression).getRightExpression();
				if (tempRightLeftExpression instanceof Parenthesis)
					tempRightLeftExpression = ((Parenthesis) tempRightLeftExpression).getExpression();
				if (tempRightRightExpression instanceof Parenthesis)
					tempRightRightExpression = ((Parenthesis) tempRightRightExpression).getExpression();
				if (tempRightLeftExpression instanceof Function) {
					if ((((Function) tempRightLeftExpression).getName().equals("remove"))) {
						binaryExpression.setRightExpression(tempRightRightExpression);
					}
				} else if (tempRightRightExpression instanceof Function) {
					if ((((Function) tempRightRightExpression).getName().equals("remove"))) {
						binaryExpression.setRightExpression(tempRightLeftExpression);
					}
				}
			}
		} else {
			Expression right = stack.pop();
			Expression left = stack.pop();
			Expression parent = stack.pop();
			((BinaryExpression) parent).setLeftExpression(left);
			((BinaryExpression) parent).setRightExpression(right);

			stack.push(parent);
		}
	}

	@Override
	public void visit(NullValue nullValue) {
		processExpression(nullValue);
	}

	@Override
	public void visit(Function function) { // should not occur anymore in normal
											// more, but occurs in tooMany Mode
		// if predicate count >35 we rename the function with the highest number
		// to 'remove(px)'
		// due to the tree structure of different types we cannot remove it
		// here, do it in the visitBinaryExpression method
		if (tooMany) {
			if (((Column) function.getParameters().getExpressions().get(0)).getWholeColumnName()
					.equals("p" + (predicateCounter - 1)) && predicateCounter > 35) {
				predicateCounter--;
				function.setName("remove");
			}
		} else
			processExpression(function);
	}

	@Override
	public void visit(InverseExpression inverseExpression) { // is a negative
																// value!
		processExpression(inverseExpression);
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
		processExpression(doubleValue);
	}

	@Override
	public void visit(LongValue longValue) {
		processExpression(longValue);
	}

	@Override
	public void visit(HexValue hexValue) {
		processExpression(hexValue);
	}

	@Override
	public void visit(DateValue dateValue) {
		processExpression(dateValue);
	}

	@Override
	public void visit(TimeValue timeValue) {
		processExpression(timeValue);
	}

	@Override
	public void visit(TimestampValue timestampValue) {
		processExpression(timestampValue);
	}

	@Override // Usually just traverse the structure, in tooMany Mode we remove
				// any function with the name 'remove(px)'
	public void visit(Parenthesis parenthesis) {
		if (tooMany) {
			if (parenthesis.getExpression() instanceof Parenthesis) {
				parenthesis.setExpression(((Parenthesis) parenthesis.getExpression()).getExpression());
			}
			parenthesis.getExpression().accept(this);
			if (parenthesis.getExpression() instanceof BinaryExpression) {
				if (((BinaryExpression) parenthesis.getExpression()).getLeftExpression() instanceof Function) {
					if ((((Function) ((BinaryExpression) parenthesis.getExpression()).getLeftExpression()).getName()
							.equals("remove"))) {
						parenthesis
								.setExpression(((BinaryExpression) parenthesis.getExpression()).getRightExpression());
					}
				}
			}
			if (parenthesis.getExpression() instanceof BinaryExpression) {
				if (((BinaryExpression) parenthesis.getExpression()).getRightExpression() instanceof Function) {
					if ((((Function) ((BinaryExpression) parenthesis.getExpression()).getRightExpression()).getName()
							.equals("remove"))) {
						parenthesis.setExpression(((BinaryExpression) parenthesis.getExpression()).getLeftExpression());
					}
				}
			}
		} else {
			stack.push(parenthesis);
			Expression tempExpression = parenthesis.getExpression();
			tempExpression.accept(this);
			tempExpression = stack.pop();
			parenthesis = (Parenthesis) stack.pop();
			parenthesis.setExpression(tempExpression);
			stack.push(parenthesis);
		}
	}

	@Override
	public void visit(StringValue stringValue) {
		processExpression(stringValue);
	}

	@Override
	public void visit(Addition addition) {
		processExpression(addition);
	}

	@Override
	public void visit(Division division) {
		processExpression(division);
	}

	@Override
	public void visit(Multiplication multiplication) {
		processExpression(multiplication);
	}

	@Override
	public void visit(Subtraction subtraction) {
		processExpression(subtraction);
	}

	@Override
	public void visit(AndExpression andExpression) {
		if (tooMany) {
			visitBinaryExpression(andExpression);
		} else if (predicateCounter > 35) { // try to limit it even before, does
											// not work as expected, problem due
											// to arbitrary structure of
											// queries, i.e., arbitrary tree
											// hierarchy
			if (andExpression.getLeftExpression() instanceof AndExpression)
				andExpression.getRightExpression().accept(this);
			else
				andExpression.getLeftExpression().accept(this);
		} else {
			stack.push(new AndExpression(null, null));
			visitBinaryExpression(andExpression);
		}
	}

	@Override
	public void visit(OrExpression orExpression) {
		if (tooMany) {
			visitBinaryExpression(orExpression);
		} else if (predicateCounter > 35) { // try to limit it even before, does
											// not work as expected, problem due
											// to arbitrary structure of
											// queries, i.e., arbitrary tree
											// hierarchy
			if (orExpression.getLeftExpression() instanceof OrExpression)
				orExpression.getRightExpression().accept(this);
			else
				orExpression.getLeftExpression().accept(this);
		} else {
			stack.push(new OrExpression(null, null));
			visitBinaryExpression(orExpression);
		}
	}

	@Override
	public void visit(Between between) {
		processExpression(between);
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		processExpression(equalsTo);
	}

	@Override
	public void visit(GreaterThan greaterThan) {
		processExpression(greaterThan);
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		processExpression(greaterThanEquals);
	}

	@Override
	public void visit(InExpression inExpression) { // two types, list or
													// subquery, is list in sdss
													// server often?

	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
		processExpression(isNullExpression);
	}

	@Override
	public void visit(LikeExpression likeExpression) {
		processExpression(likeExpression);
	}

	@Override
	public void visit(MinorThan minorThan) {
		processExpression(minorThan);
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		processExpression(minorThanEquals);
	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		processExpression(notEqualsTo);
	}

	@Override
	public void visit(Column tableColumn) {

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
	}

	@Override
	public void visit(Matches matches) {
	}

	@Override
	public void visit(BitwiseAnd bitwiseAnd) {
	}

	@Override
	public void visit(BitwiseOr bitwiseOr) {
	}

	@Override
	public void visit(BitwiseXor bitwiseXor) {
	}

	@Override
	public void visit(CastExpression cast) {
	}

	@Override
	public void visit(Modulo modulo) {
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