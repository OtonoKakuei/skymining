/* Written by FlorianB for SQLQueryLogTransformer:
 * This file is for parsing a SQL SELECT statement and extracting the addressed relations and used constraints
 * It will process the different clauses (only select, from, where and having) and union or intersect each access area
 */
package accessarea;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.lang3.StringUtils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FunctionItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;
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
import net.sf.jsqlparser.statement.select.*;

public class SQLParser implements SelectItemVisitor, ExpressionVisitor, SelectVisitor { // FromItemVisitor,
																						// ItemsListVisitor

	private Boolean withUDF = true;
	private ExpressionVisitor expressionVisitor;

	public AccessArea parseStmtWithoutUDFs(String statement) throws JSQLParserException {
		withUDF = false;
		NullValue initExpression = new NullValue();
		AccessArea accessArea = new AccessArea(new ArrayList<FromItem>(), initExpression);

		Statement stmt = null;
		// Parse via jSQLParser
		try {
			stmt = CCJSqlParserUtil.parse(statement);
		} catch (Exception e) {
			// e.printStackTrace();
			return null;
		}
		// jSQL parser uses multiple constructs, first a Statement, this can
		// then be a select, update, create etc. statement
		Select selectStatement = (Select) stmt; // We are only interested in
												// select statements

		// A Select statement consists of a plainselect type
		PlainSelect plainStatement = (PlainSelect) selectStatement.getSelectBody();

		// Parse From clause:
		accessArea = parseFrom(plainStatement.getFromItem(), plainStatement.getJoins());

		// Parse where clause:
		AccessArea accessAreaWhere = parseWhere(plainStatement.getWhere());

		// From and WHERE access areas are interesected (i.e., conjunction)
		accessArea = intersectAccessAreas(accessAreaWhere, accessArea);

		// Parse HAVING clause
		AccessArea accessAreaHaving = parseHaving(plainStatement.getHaving());
		accessArea = intersectAccessAreas(accessAreaHaving, accessArea);

		// SELECT clause as last one since this has no influence on the other
		// constraints and is connected by conjunction instead of disjunction
		AccessArea accessAreaSelect = parseSelect(plainStatement.getSelectItems());
		accessArea = unionAccessAreas(accessAreaSelect, accessArea);

		return accessArea;
	}

	public AccessArea parseStmt(String statement) throws JSQLParserException {

		NullValue initExpression = new NullValue();
		AccessArea accessArea = new AccessArea(new ArrayList<FromItem>(), initExpression);

		Statement stmt = null;
		// Parse via jSQLParser
		try {
			stmt = CCJSqlParserUtil.parse(statement);
		} catch (Exception e) {
			// e.printStackTrace();
			return null;
		}
		// jSQL parser uses multiple constructs, first a Statement, this can
		// then be a select, update, create etc. statement
		Select selectStatement = (Select) stmt; // We are only interested in
												// select statements

		// A Select statement consists of a plainselect type
		PlainSelect plainStatement = (PlainSelect) selectStatement.getSelectBody();

		// Parse From clause:
		accessArea = parseFrom(plainStatement.getFromItem(), plainStatement.getJoins());

		// Parse where clause:
		AccessArea accessAreaWhere = parseWhere(plainStatement.getWhere());

		// From and WHERE access areas are interesected (i.e., conjunction)
		accessArea = intersectAccessAreas(accessAreaWhere, accessArea);

		// Parse HAVING clause
		AccessArea accessAreaHaving = parseHaving(plainStatement.getHaving());
		accessArea = intersectAccessAreas(accessAreaHaving, accessArea);

		// SELECT clause as last one since this has no influence on the other
		// constraints and is connected by conjunction instead of disjunction
		AccessArea accessAreaSelect = parseSelect(plainStatement.getSelectItems());
		accessArea = unionAccessAreas(accessAreaSelect, accessArea);

		return accessArea;
	}

	// Get access areas from any subquery in select clause:
	public AccessArea parseSelect(List<SelectItem> select) throws JSQLParserException {

		// Iterator moves along the SELECT items
		ListIterator<SelectItem> it = select.listIterator();
		SelectItem item;
		String itemString;
		NullValue initExpression = new NullValue();
		AccessArea selectAccessArea = new AccessArea(new ArrayList<FromItem>(), initExpression);

		while (it.hasNext()) {
			item = it.next();
			// make the current item an Expression of the jSQL parser
			item.accept(this);
			if (!(item instanceof AllColumns || item instanceof AllTableColumns)) {
				// A Function can also mean new relations and constraints if it
				// is an sdss special function. transform it via our converter
				if (((SelectExpressionItem) item).getExpression() instanceof Function) {
					Converter converter = new Converter();
					AccessArea tempAccessArea = converter
							.convertPredicate(((SelectExpressionItem) item).getExpression());
					if (!(tempAccessArea.getWhere() instanceof BinaryExpression))
						tempAccessArea.setWhere(null);
					selectAccessArea = unionAccessAreas(tempAccessArea, selectAccessArea);
				}
				// if the current item contains "select" i.e. it is a subquery,
				// then parse the subquery with the parseStmt from 'this' class
				// and union the access areas
				itemString = ((SelectExpressionItem) item).getExpression().toString();
				if (StringUtils.containsIgnoreCase(itemString, "select")) {
					itemString = itemString.substring(1, itemString.length() - 1);
					selectAccessArea = unionAccessAreas(parseStmt(itemString), selectAccessArea);
				}
			}
		}
		return selectAccessArea;
	}

	// Get the relations and any subquery and sdss special function from the
	// FROM clause
	public AccessArea parseFrom(FromItem from, List<Join> joins) throws JSQLParserException {
		Expression where = null;
		AndExpression andExpression = new AndExpression(null, null);
		List<FromItem> tables = new ArrayList<FromItem>();
		NullValue initExpression = new NullValue();
		AccessArea fromAccessArea = new AccessArea(new ArrayList<FromItem>(), initExpression);
		if (from instanceof FunctionItem) {// Get SDSS special functions,
											// transform via converter
			if (!withUDF) {
				FunctionItem t = (FunctionItem) from;
				if (t.getAlias() != null) {
					String alias = t.getAlias();
					if (alias.startsWith("[") && alias.endsWith("]")
							|| alias.startsWith("\"") && alias.endsWith("\"")) {
						alias = alias.substring(1, alias.length() - 1);
						t.setAlias(alias);

					}
					tables.add(from);
				}
			} else {
				Converter converter = new Converter();
				AccessArea tempArea = converter.convertPredicate(((FunctionItem) from));
				if (tempArea.getFrom() != null)
					tables.addAll(tempArea.getFrom());
				if (where == null) {
					if (tempArea.getWhere() instanceof BooleanValue) {
						if (((BooleanValue) tempArea.getWhere()).getValue() == true) {
						} else
							where = tempArea.getWhere();
					} else
						where = tempArea.getWhere();
				}
			}
		} else if (from instanceof Table) {
			Table t = (Table) from;
			if (t.getAlias() != null) {
				String alias = t.getAlias();
				if (alias.startsWith("[") && alias.endsWith("]") || alias.startsWith("\"") && alias.endsWith("\"")) {
					alias = alias.substring(1, alias.length() - 1);
					t.setAlias(alias);
				}
			}
			if (t.getName() != null) {
				String name = t.getName();
				if (name.startsWith("[") && name.endsWith("]") || name.startsWith("\"") && name.endsWith("\"")) {
					name = name.substring(1, name.length() - 1);
					t.setName(name);
				}
			}
			tables.add(t);
		} else if (from instanceof FromItem) {
			if (!(from instanceof SubJoin) && !(from instanceof SubSelect)) {
				tables.add(from);
			} else if (from instanceof SubJoin) {// Get Subjoin
				List<Join> subJoinList = new ArrayList<Join>();
				subJoinList.add(((SubJoin) from).getJoin());
				AccessArea subJoinArea = parseFrom(((SubJoin) from).getLeft(), subJoinList);
				if (fromAccessArea.getFrom().isEmpty() && fromAccessArea.getWhere() instanceof NullValue)
					fromAccessArea = subJoinArea;
				else {
					fromAccessArea = intersectAccessAreas(subJoinArea, fromAccessArea);
				}
			} else if (from instanceof SubSelect) {// Get Subselect
				AccessArea subSelectArea = parseStmt(((SubSelect) from).getSelectBody().toString());
				if (fromAccessArea.getFrom().isEmpty() && fromAccessArea.getWhere() instanceof NullValue)
					fromAccessArea = subSelectArea;
				else {
					fromAccessArea = intersectAccessAreas(subSelectArea, fromAccessArea);
				}
			}
		}
		// Get all Joins iteratively
		if (joins != null) {

			ListIterator<Join> it = joins.listIterator();
			Join join;

			while (it.hasNext()) {
				join = it.next();
				// Each JOIN has a left side and a right side, transform with
				// converter, always check if returned constraints are Boolean
				// values, we need to handle them
				if (join.getRightItem() instanceof FunctionItem) {
					if (!withUDF) {
						FunctionItem t = (FunctionItem) join.getRightItem();
						if (t.getAlias() != null) {
							String alias = t.getAlias();
							if (alias.startsWith("[") && alias.endsWith("]")
									|| alias.startsWith("\"") && alias.endsWith("\"")) {
								alias = alias.substring(1, alias.length() - 1);
								t.setAlias(alias);
							}
						}
						tables.add(join.getRightItem());
					} else {
						Converter converter = new Converter();
						AccessArea tempArea = converter.convertPredicate((FunctionItem) join.getRightItem());
						if (tempArea.getFrom() != null)
							tables.addAll(tempArea.getFrom());
						if (where == null) {
							if (tempArea.getWhere() instanceof BooleanValue) {
								if (((BooleanValue) tempArea.getWhere()).getValue() == true) {
								} else
									where = tempArea.getWhere();
							} else
								where = tempArea.getWhere();
						} else if (tempArea.getWhere() != null) {
							andExpression = new AndExpression(null, null);
							andExpression.setLeftExpression(where);
							andExpression.setRightExpression(tempArea.getWhere());
							where = andExpression;
						}
					}
				} else if (join.getRightItem() instanceof Table) {
					Table t = (Table) join.getRightItem();
					if (t.getAlias() != null) {
						String alias = t.getAlias();
						if (alias.startsWith("[") && alias.endsWith("]")
								|| alias.startsWith("\"") && alias.endsWith("\"")) {
							alias = alias.substring(1, alias.length() - 1);
							t.setAlias(alias);
						}
					}
					if (t.getName() != null) {
						String name = t.getName();
						if (name.startsWith("[") && name.endsWith("]")
								|| name.startsWith("\"") && name.endsWith("\"")) {
							name = name.substring(1, name.length() - 1);
							t.setName(name);
						}
					}
					tables.add(t);
				} else if (join.getRightItem() instanceof FromItem) {
					if (!(join.getRightItem() instanceof SubJoin) && !(join.getRightItem() instanceof SubSelect)) {
						tables.add(join.getRightItem());
					} else if (join.getRightItem() instanceof SubJoin) {
						AccessArea subJoinArea = parseFrom(join.getRightItem(), null);
						if (fromAccessArea.getFrom().isEmpty() && fromAccessArea.getWhere() instanceof NullValue)
							fromAccessArea = subJoinArea;
						else {
							fromAccessArea = intersectAccessAreas(subJoinArea, fromAccessArea);
						}
					} else if (join.getRightItem() instanceof SubSelect) {
						AccessArea subSelectArea = parseStmt(
								((SubSelect) join.getRightItem()).getSelectBody().toString());
						if (fromAccessArea.getFrom().isEmpty() && fromAccessArea.getWhere() instanceof NullValue)
							fromAccessArea = subSelectArea;
						else {
							fromAccessArea = intersectAccessAreas(subSelectArea, fromAccessArea);
						}
					}
				}
				// Push join conditions to where clause
				if (join.getOnExpression() != null && !join.isFull() && !join.isLeft() && !join.isRight()) {
					Converter converter = new Converter();
					AccessArea tempArea = converter.convertPredicate(join.getOnExpression());
					if (tempArea.getFrom() != null)
						tables.addAll(tempArea.getFrom());
					if (where == null)
						where = tempArea.getWhere();
					else if (tempArea.getWhere() != null) {
						if (!(tempArea.getWhere() instanceof BooleanValue)) {
							andExpression = new AndExpression(null, null);
							andExpression.setLeftExpression(where);
							andExpression.setRightExpression(tempArea.getWhere());
							where = andExpression;
						}
					}

				}
			}
		}
		// Either merge the multiple relations and constraints if they exist, or
		// return just the one
		if (fromAccessArea.getFrom().isEmpty() && fromAccessArea.getWhere() instanceof NullValue)
			fromAccessArea = new AccessArea(tables, where);
		else {
			fromAccessArea = intersectAccessAreas(new AccessArea(tables, where), fromAccessArea);
		}
		return fromAccessArea;
	}

	// Send WHERE clause to converter
	public AccessArea parseWhere(Expression where) throws JSQLParserException {

		NullValue initExpression = new NullValue();
		AccessArea whereAccessArea = new AccessArea(new ArrayList<FromItem>(), initExpression);

		if (where != null && !(where instanceof NullValue)) {
			Converter converter = new Converter();
			whereAccessArea = converter.convertPredicate(where);
		}
		return whereAccessArea;
	}

	// HAVING clause to converter
	public AccessArea parseHaving(Expression having) throws JSQLParserException {

		NullValue initExpression = new NullValue();
		AccessArea havingAccessArea = new AccessArea(new ArrayList<FromItem>(), initExpression);

		if (having != null && !(having instanceof NullValue)) {
			Converter converter = new Converter();
			havingAccessArea = converter.convertPredicate(having);
		}
		return havingAccessArea;
	}

	// Merge FROM clauses and constraints in a union
	public AccessArea unionAccessAreas(AccessArea tempAccessArea, AccessArea accessArea) {
		OrExpression tempExpr = new OrExpression(null, null);
		if (tempAccessArea.getFrom() != null) {
			List<FromItem> tempFromItemList = accessArea.getFrom();
			tempFromItemList.addAll(tempAccessArea.getFrom());
			accessArea.setFrom(tempFromItemList);
		}
		if (!(tempAccessArea.getWhere() instanceof NullValue || tempAccessArea.getWhere() == null)) {
			if (tempAccessArea.getWhere() instanceof BooleanValue
					&& ((BooleanValue) tempAccessArea.getWhere()).getValue())
				accessArea.setWhere(tempAccessArea.getWhere());
			else if (tempAccessArea.getWhere() instanceof BooleanValue
					&& !((BooleanValue) tempAccessArea.getWhere()).getValue()) {
				if (accessArea.getWhere() == null || accessArea.getWhere() instanceof NullValue)
					accessArea.setWhere(tempAccessArea.getWhere());
				else
					accessArea.setWhere(accessArea.getWhere());
			} else if (!(accessArea.getWhere() instanceof NullValue || accessArea.getWhere() == null)) {
				if (accessArea.getWhere() instanceof BooleanValue && !((BooleanValue) accessArea.getWhere()).getValue())
					accessArea.setWhere(tempAccessArea.getWhere());
				else if (accessArea.getWhere() instanceof BooleanValue
						&& ((BooleanValue) accessArea.getWhere()).getValue())
					accessArea.setWhere(accessArea.getWhere());
				else {
					if (accessArea.getWhere() instanceof Parenthesis || !(accessArea.getWhere() instanceof OrExpression
							|| accessArea.getWhere() instanceof AndExpression))
						tempExpr.setLeftExpression(accessArea.getWhere());
					else {
						Parenthesis tempParenthesis = new Parenthesis();
						tempParenthesis.setExpression(accessArea.getWhere());
						tempExpr.setLeftExpression(tempParenthesis);
					}
					tempExpr.setRightExpression(tempAccessArea.getWhere());
					accessArea.setWhere(tempExpr);
				}
			} else {
				accessArea.setWhere(tempAccessArea.getWhere());
			}
		}
		return accessArea;
	}

	// Merge in an intersection (conjunction)
	public AccessArea intersectAccessAreas(AccessArea tempAccessArea, AccessArea accessArea) {
		AndExpression tempExpr = new AndExpression(null, null);
		if (tempAccessArea.getFrom() != null) {
			List<FromItem> tempFromItemList = accessArea.getFrom();
			tempFromItemList.addAll(tempAccessArea.getFrom());
			accessArea.setFrom(tempFromItemList);
		}
		if (!(tempAccessArea.getWhere() instanceof NullValue || tempAccessArea.getWhere() == null)) {
			if (tempAccessArea.getWhere() instanceof BooleanValue
					&& !((BooleanValue) tempAccessArea.getWhere()).getValue())
				accessArea.setWhere(tempAccessArea.getWhere());
			else if (tempAccessArea.getWhere() instanceof BooleanValue
					&& ((BooleanValue) tempAccessArea.getWhere()).getValue()) {
				if (accessArea.getWhere() == null || accessArea.getWhere() instanceof NullValue)
					accessArea.setWhere(tempAccessArea.getWhere());
				else
					accessArea.setWhere(accessArea.getWhere());
			} else if (!(accessArea.getWhere() instanceof NullValue || accessArea.getWhere() == null)) {
				if (accessArea.getWhere() instanceof BooleanValue && ((BooleanValue) accessArea.getWhere()).getValue())
					accessArea.setWhere(tempAccessArea.getWhere());
				else if (accessArea.getWhere() instanceof BooleanValue
						&& !((BooleanValue) accessArea.getWhere()).getValue())
					accessArea.setWhere(accessArea.getWhere());
				else {
					if (accessArea.getWhere() instanceof Parenthesis
							|| !(accessArea.getWhere() instanceof OrExpression))
						tempExpr.setLeftExpression(accessArea.getWhere());
					else {
						Parenthesis tempParenthesis = new Parenthesis();
						tempParenthesis.setExpression(accessArea.getWhere());
						tempExpr.setLeftExpression(tempParenthesis);
					}
					tempExpr.setRightExpression(tempAccessArea.getWhere());
					accessArea.setWhere(tempExpr);
				}
			} else {
				accessArea.setWhere(tempAccessArea.getWhere());
			}
		}
		return accessArea;
	}

	@Override
	public void visit(SubSelect subSelect) {
		subSelect.getSelectBody().accept(this);
	}

	@Override
	public void visit(AllColumns allColumns) {
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		selectExpressionItem.getExpression().accept(this);
	}

	@Override
	public void visit(PlainSelect plainSelect) {
	}

	@Override
	public void visit(SetOperationList setOpList) {
	}

	@Override
	public void visit(WithItem withItem) {
	}

	public ExpressionVisitor getExpressionVisitor() {
		return expressionVisitor;
	}

	public void setExpressionVisitor(ExpressionVisitor visitor) {
		expressionVisitor = visitor;
	}

	@Override
	public void visit(NullValue nullValue) {
	}

	@Override
	public void visit(Function function) {
	}

	@Override
	public void visit(InverseExpression inverseExpression) {
	}

	@Override
	public void visit(JdbcParameter jdbcParameter) {
	}

	@Override
	public void visit(JdbcNamedParameter jdbcNamedParameter) {
	}

	@Override
	public void visit(DoubleValue doubleValue) {
	}

	@Override
	public void visit(LongValue longValue) {
	}

	@Override
	public void visit(HexValue hexValue) {
	}

	@Override
	public void visit(DateValue dateValue) {
	}

	@Override
	public void visit(TimeValue timeValue) {
	}

	@Override
	public void visit(TimestampValue timestampValue) {
	}

	@Override
	public void visit(Parenthesis parenthesis) {
	}

	@Override
	public void visit(StringValue stringValue) {
	}

	@Override
	public void visit(Addition addition) {
	}

	@Override
	public void visit(Division division) {
	}

	@Override
	public void visit(Multiplication multiplication) {
	}

	@Override
	public void visit(Subtraction subtraction) {
	}

	@Override
	public void visit(AndExpression andExpression) {
	}

	@Override
	public void visit(OrExpression orExpression) {
	}

	@Override
	public void visit(Between between) {
	}

	@Override
	public void visit(EqualsTo equalsTo) {
	}

	@Override
	public void visit(GreaterThan greaterThan) {
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
	}

	@Override
	public void visit(InExpression inExpression) {
	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
	}

	@Override
	public void visit(LikeExpression likeExpression) {
	}

	@Override
	public void visit(MinorThan minorThan) {
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {
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
	public void visit(BooleanValue booleanValue) {
	}
}
