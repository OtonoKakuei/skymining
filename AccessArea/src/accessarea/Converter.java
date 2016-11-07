/* Written by FlorianB for SQLQueryLogTransformer:
 * This is the most important class, here we traverse the hierarchy of the current statement a first time and extract all the predicates (and relations and predicates from any subquery)
 * We start with the given expression and do a depth-first search and always keep a predicate or transform it or extract the access area form a subquery
 */
package accessarea;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Stack;

import net.sf.jsqlparser.JSQLParserException;
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
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.FunctionItem;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;

public class Converter
		implements SelectVisitor, SelectItemVisitor, FromItemVisitor, ExpressionVisitor, ItemsListVisitor {

	private ExpressionVisitor expressionVisitor;
	private Stack<Expression> stack = new Stack<Expression>();
	private NullValue initExpression = new NullValue();
	private AccessArea accessArea = new AccessArea(new ArrayList<FromItem>(), initExpression);
	private Expression subWhere;
	private int fPhotoFlags = 0;

	private String alias = "";

	public AccessArea convertPredicate(FunctionItem func) {
		Expression predicate = func.getFunction();
		alias = func.getAlias();
		predicate.accept(this); // here we traverse the hierarchy-> start!
		Expression where = stack.pop();
		// subwhere are the constraints from any subquery that was not in
		// combination with any IN, EXISTS, ALL, ANY...
		if (subWhere != null) {// have to merge it with the rest of the access
								// area
			if (where != null && !(where instanceof BooleanValue)) {
				AndExpression tempAndExpression = new AndExpression(null, null);
				if (where instanceof Parenthesis || !(where instanceof OrExpression))
					tempAndExpression.setLeftExpression(where);
				else {
					Parenthesis tempParenthesis = new Parenthesis();
					tempParenthesis.setExpression(where);
					tempAndExpression.setLeftExpression(tempParenthesis);
				}
				tempAndExpression.setRightExpression(subWhere);
				where = tempAndExpression;
			} else if (where instanceof BooleanValue) {
				if (((BooleanValue) where).getValue())
					where = subWhere;
			} else
				where = subWhere;
		}
		if (!(where instanceof Parenthesis) && (where instanceof OrExpression)) {// Make
																					// a
																					// parenthesis
																					// around
																					// the
																					// where
																					// if
																					// it
																					// is
																					// an
																					// OR
																					// expression
			Parenthesis tempParenthesis = new Parenthesis();
			tempParenthesis.setExpression(where);
			where = tempParenthesis;
		}
		accessArea.setWhere(where);
		return accessArea;
	}

	public AccessArea convertPredicate(Expression predicate) {
		predicate.accept(this); // here we traverse the hierarchy-> start!
		Expression where = stack.pop();
		// subwhere are the constraints from any subquery that was not im
		// combination with any IN, EXISTS, ALL, ANY...
		if (subWhere != null) {// have to merge it with the est of the access
								// area
			if (where != null && !(where instanceof BooleanValue)) {
				AndExpression tempAndExpression = new AndExpression(null, null);
				if (where instanceof Parenthesis || !(where instanceof OrExpression))
					tempAndExpression.setLeftExpression(where);
				else {
					Parenthesis tempParenthesis = new Parenthesis();
					tempParenthesis.setExpression(where);
					tempAndExpression.setLeftExpression(tempParenthesis);
				}
				tempAndExpression.setRightExpression(subWhere);
				where = tempAndExpression;
			} else if (where instanceof BooleanValue) {
				if (((BooleanValue) where).getValue())
					where = subWhere;
			} else
				where = subWhere;
		}
		if (!(where instanceof Parenthesis) && (where instanceof OrExpression)) {// Make
																					// a
																					// parenthesis
																					// around
																					// the
																					// where
																					// if
																					// it
																					// is
																					// an
																					// OR
																					// expression
			Parenthesis tempParenthesis = new Parenthesis();
			tempParenthesis.setExpression(where);
			where = tempParenthesis;
		}
		accessArea.setWhere(where);
		return accessArea;
	}

	// for any binary expression this function processes the right hand side and
	// left hand side of it
	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		Expression tempLeftExpression = binaryExpression.getLeftExpression();
		Expression tempRightExpression = binaryExpression.getRightExpression();
		if (((BinaryExpression) stack.peek()).isNot()) {// handle any NOT
			if (tempLeftExpression instanceof BinaryExpression) {
				((BinaryExpression) tempLeftExpression).toggleNot();
			}
			if (tempRightExpression instanceof BinaryExpression) {
				((BinaryExpression) tempRightExpression).toggleNot();
			}
		}
		// if left side is a function we need to reverse the order
		if (tempLeftExpression instanceof Function)
			stack.push(tempRightExpression);
		tempLeftExpression.accept(this);
		if (!(tempLeftExpression instanceof Function))
			tempRightExpression.accept(this);

		// combine the parent element with its right hand side and left hand
		// side:
		Expression right = stack.pop();
		Expression left = stack.pop();
		Expression parent = stack.pop();

		// we have to handle if any element was deleted or set to TRUE or FALSE.
		// On the hierarchy we cannot change the type of an expression itself,
		// therefore we have to handle it here and construct a new parent
		// expression
		Expression expression = null;
		if (parent != null) {
			if (right == null)
				expression = left;
			else if (left == null)
				expression = right;
			else if (right instanceof BooleanValue && !(parent instanceof BooleanValue)) {
				if (((BooleanValue) right).getValue() == true) {
					if (parent instanceof AndExpression && (left instanceof MinorThanEquals || left instanceof MinorThan
							|| left instanceof GreaterThan || left instanceof GreaterThanEquals
							|| left instanceof IsNullExpression || left instanceof EqualsTo
							|| left instanceof NotEqualsTo || left instanceof LikeExpression
							|| left instanceof AndExpression || left instanceof OrExpression))
						expression = left;
					else
						expression = right;
				} else {
					if (parent instanceof AndExpression)
						expression = right;
					else if (parent instanceof OrExpression && (left instanceof MinorThanEquals
							|| left instanceof MinorThan || left instanceof GreaterThan
							|| left instanceof GreaterThanEquals || left instanceof IsNullExpression
							|| left instanceof EqualsTo || left instanceof NotEqualsTo || left instanceof LikeExpression
							|| left instanceof AndExpression || left instanceof OrExpression))
						expression = left;
					else
						expression = right;
				}
			} else if (left instanceof BooleanValue && !(parent instanceof BooleanValue)) {
				if (((BooleanValue) left).getValue() == true) {
					if (parent instanceof AndExpression
							&& (right instanceof MinorThanEquals || right instanceof MinorThan
									|| right instanceof GreaterThan || right instanceof GreaterThanEquals
									|| right instanceof IsNullExpression || right instanceof EqualsTo
									|| right instanceof NotEqualsTo || right instanceof LikeExpression
									|| right instanceof AndExpression || right instanceof OrExpression))
						expression = right;
					else
						expression = left;
				} else {
					if (parent instanceof AndExpression)
						expression = left;
					else if (parent instanceof OrExpression
							&& (right instanceof MinorThanEquals || right instanceof MinorThan
									|| right instanceof GreaterThan || right instanceof GreaterThanEquals
									|| right instanceof IsNullExpression || right instanceof EqualsTo
									|| right instanceof NotEqualsTo || right instanceof LikeExpression
									|| right instanceof AndExpression || right instanceof OrExpression))
						expression = right;
					else
						expression = left;
				}
			} else if (!(parent instanceof BooleanValue)) {
				if (((BinaryExpression) parent).isNot())
					((BinaryExpression) parent).setNot(false);
				((BinaryExpression) parent).setLeftExpression(left);
				((BinaryExpression) parent).setRightExpression(right);
				expression = parent;
			} else
				expression = parent;
		} else
			expression = parent;
		stack.push(expression);
	}

	@Override
	public void visit(ExpressionList expressionList) {
		stack.push((Expression) expressionList);
	}

	@Override
	public void visit(MultiExpressionList multiExprList) {
		stack.push((Expression) multiExprList);
	}

	@Override
	public void visit(NullValue nullValue) {
		stack.push(nullValue);
	}

	@Override
	public void visit(Function function) {// Transform aggregate functions and
											// SDSS special functions
		String name = function.getName().toLowerCase();
		if (name.startsWith("dbo."))
			name = name.substring(4, name.length());
		Expression right = null;
		Expression parent = null;
		if (!stack.isEmpty()) {
			right = stack.pop();
			parent = stack.pop();
		}
		if (name.toLowerCase().startsWith("f")) { // is SDSS special function
			Float ra1 = null;
			Float ra2 = null;
			Float dec1 = null;
			Float dec2 = null;
			Float r = null;
			String ObjectTable = null;
			switch (name) {
			// case "fdistanceeq": //returns distance (arcmins) between two
			// points (ra1,dec1) and (ra2,dec2), usually used with attribute as
			// parameter, needs to be calculated for each value pair
			// only 105 times in FROM OR WHERE PART, else in SELECT
			// break;
			case "fdoccolumns": // Return the list of Columns in a given table
								// or view, i.e.: accesses the schema, returns
								// no rows, only used in FROM part
								// Proposal: add table to list of accessed
								// tables, add FALSE as no tuples would
								// influence the result
								// This function ONLY occurs in statements of
								// the following form: SELECT * FROM
								// dbo.fDocColumns('[...]')
								// Where [..] is the name of some table or view.
								// No other tables or predicates in these
								// queries
								// Accesses DBColumns table to get descriptions
				String tableString = function.getParameters().getExpressions().get(0).toString();
				Table table = new Table(null, tableString.substring(1, tableString.length() - 1));
				stack.push(new BooleanValue(false));
				List<FromItem> tempFromItemList = accessArea.getFrom();
				FromItem fromItem = table;
				tempFromItemList.add(fromItem);
				table = new Table(null, "DBColumns");
				fromItem = table;
				tempFromItemList.add(fromItem);
				accessArea.setFrom(tempFromItemList);
				// System.out.println(table);
				break;
			case "fdocfunctionparams": // Return the parameters of a function,
										// by definition this does not access
										// any table
				// But for looks we return the Tablename DocFunctionParams and
				// the expression: where functionName = '...'
				EqualsTo tempEqualsTo = new EqualsTo();
				tempEqualsTo.setLeftExpression(new Column(new Table(null, null), "functionName"));
				tempEqualsTo.setRightExpression(
						new StringValue(function.getParameters().getExpressions().get(0).toString()));
				stack.push(tempEqualsTo);
				List<FromItem> tempFromItemList2 = accessArea.getFrom();
				table = new Table(null, "DocFunctionParams");
				tempFromItemList2.add(table);
				accessArea.setFrom(tempFromItemList2);
				// System.out.println(table);
				break;
			case "ffootprinteq": // Determines whether a point is inside the
									// survey footprint
									// Returns only POLYGON String or Empty
									// String
									// Only 131 times in queries with a where
									// clause, else always in FROM Part in
									// queries without where clause and having
									// clause as well
									// Accesses unknown table to check if the
									// given area described by the parameters
									// ra, dec, radius in arcmin is covered by
									// sdss footage
				// But for looks we return the Tablename SDSSFootPrint and the
				// expression: where ra = ... and dec = ... and radius = ...
				// System.out.println(function.getParameters().getExpressions());
				EqualsTo tempEqualsTo2 = new EqualsTo();
				tempEqualsTo2.setLeftExpression(new Column(new Table(null, null), "ra"));
				tempEqualsTo2.setRightExpression(function.getParameters().getExpressions().get(0));
				EqualsTo tempEqualsTo3 = new EqualsTo();
				tempEqualsTo3.setLeftExpression(new Column(new Table(null, null), "dec"));
				tempEqualsTo3.setRightExpression(function.getParameters().getExpressions().get(1));
				EqualsTo tempEqualsTo4 = new EqualsTo();
				tempEqualsTo4.setLeftExpression(new Column(new Table(null, null), "radius"));
				tempEqualsTo4.setRightExpression(function.getParameters().getExpressions().get(2));
				stack.push(new AndExpression(new AndExpression(tempEqualsTo2, tempEqualsTo3), tempEqualsTo4));
				List<FromItem> tempFromItemList3 = accessArea.getFrom();
				table = new Table(null, "SDSSFootPrint");
				tempFromItemList3.add(table);
				accessArea.setFrom(tempFromItemList3);
				// System.out.println(table);
				break;
			case "fgetnearbyobjalleq": // Returns a table of all objects within
										// @r arcmins of the point. There is no
										// limit on @r.
										// Note with respect to the following
										// functions: Table All refers to
										// primary, secondary and other objects
										// One arcmin is 0.0166666666667 degree
										// cos(A) = sin(dec1)sin(dec2) +
										// cos(dec1)cos(dec2)cos(ra1-ra2) -> A =
										// distance between the two points in
										// degrees
										// Only 1 time occurs more than once in
										// a query, therefore not handled
										// Rewritten as SuperSet: SELECT * FROM
										// PhotoObjAll where ra between 185-0.05
										// and 185+0.05 and dec between -0.05
										// and 0.05
				if (ObjectTable == null)
					ObjectTable = "PhotoPrimary";
			case "fgetnearestobjeq": // Returns a table holding a record
										// describing the closest primary object
										// within @r arcminutes of (@ra,@dec).
				// Returns a table with a single row!
				// But accesses the same objects as fgetnearbyobjeq -> only
				// orders the list by distance and outputs only top 1
				// Only 49 times occurs more than once in a query, therefore
				// multiple occurrences not handled yet
				if (ObjectTable == null)
					ObjectTable = "PhotoPrimary"; // Only SuperSet
			case "fgetnearbyframeeq": // Returns table with a record describing
										// the frames neareby (@ra,@dec) at a
										// given @zoom level.
				// Returns a table with a single row!
				// But accesses the same objects as fgetnearbyobjeq -> only
				// orders the list by distance and outputs only top 1
				// Only 49 times occurs more than once in a query, therefore
				// multiple occurrences not handled yet
				if (ObjectTable == null)
					ObjectTable = "Frame"; // Only SuperSet

			case "fgetnearbyobjeq": // Returns a table of primary objects within
									// @r arcmins of the point. There is no
									// limit on @r.
									// Only 370 times occurs more than once in a
									// query, therefore not handled
				if (ObjectTable == null)
					ObjectTable = "PhotoPrimary"; // Only SuperSet

			case "fgetnearbyspecobjalleq": // Returns a table of spectrum
											// objects within @r arcmins of an
											// equatorial point (@ra, @dec).
											// (Primary, Secondary, other)
											// Never occurs more than once in a
											// query, therefore not handled
				if (ObjectTable == null)
					ObjectTable = "SpecObjAll"; // Only SuperSet
			case "fgetnearbyapogeestareq": // Returns table of spectrum objects
											// within @r arcmins of an
											// equatorial point (@ra, @dec).
											// (Only Primary)
				// fGetNearbyApogeeStarEq Only 48 time occurs more than once in
				// a query, therefore not handled
				if (ObjectTable == null)
					ObjectTable = "apogeeStar"; // Only SuperSet
			case "fgetnearbyspecobjeq": // Returns table of spectrum objects
										// within @r arcmins of an equatorial
										// point (@ra, @dec). (Only Primary)
										// Only 48 time occurs more than once in
										// a query, therefore not handled
				if (ObjectTable == null)
					ObjectTable = "SpecObj"; // Only SuperSet
				ra1 = Float.parseFloat(function.getParameters().getExpressions().get(0).toString());
				dec1 = Float.parseFloat(function.getParameters().getExpressions().get(1).toString());
				r = Math.abs((float) ((Float.parseFloat(function.getParameters().getExpressions().get(2).toString()))
						* 0.0166666666667));
				// String subQueryR1 = "SELECT * FROM PhotoPrimary as "+alias+"
				// where "+alias+".ra between "+Math.min(ra1, ra2)+" and
				// "+Math.max(ra1, ra2)+" and "+alias+".dec between
				// "+Math.min(dec1, dec2)+" and "+Math.max(dec1, dec2);

				String subQueryO = "SELECT * FROM " + ObjectTable + " as " + alias + " where " + alias + ".ra between "
						+ (ra1 - r) + " and " + (ra1 + r) + " and " + alias + ".dec between " + (dec1 - r) + " and "
						+ (dec1 + r);
				if (name.equalsIgnoreCase("fgetnearbyframeeq")) {
					Integer zoom = Integer.parseInt(function.getParameters().getExpressions().get(3).toString());
					subQueryO = subQueryO + " and " + alias + ".zoom = " + (zoom);
				}
				try {
					stack.push(parent);
					Statement stmt = CCJSqlParserUtil.parse(subQueryO);
					Select selectStatement = (Select) stmt;
					SubSelect subSelect = new SubSelect();
					subSelect.setSelectBody((PlainSelect) selectStatement.getSelectBody());
					subSelect.accept((ExpressionVisitor) this);
					if (right != null)
						right.accept(this);
					else
						stack.pop();
				} catch (JSQLParserException e) {
					// System.out.println("Could not parse Subquery from
					// function: "+function);
					// e.printStackTrace();
				}
				break;

			case "fgetobjfromrect": // Returns a table of objects inside a
									// rectangle defined by two ra,dec pairs.
									// Note the order of the parameters: @ra1,
									// @ra2, @dec1, @dec2
									// See fgetobjfromrecteq for conversion
				ra2 = Float.parseFloat(function.getParameters().getExpressions().get(1).toString());
				dec1 = Float.parseFloat(function.getParameters().getExpressions().get(2).toString());

			case "fgetobjfromrecteq":// Returns a table of objects inside a
										// rectangle defined by two ra,dec
										// pairs. Note the order of the
										// parameters: @ra1, @dec1, @ra2, @dec2
										// This is the same as fgetobjfromrect
										// expect for the order of parameters!
										// Both can be converted to select *
										// from PhotoPrimary where ra between
										// 185 and 185.1 and dec between 0 and
										// 0.1
										// Both occur only a total of 7 times
										// more than once in one query.
										// Therefore we leave the handling of
										// multiple occurrences for now
				if (ObjectTable == null)
					ObjectTable = "PhotoPrimary";
				ra1 = Float.parseFloat(function.getParameters().getExpressions().get(0).toString());
				dec2 = Float.parseFloat(function.getParameters().getExpressions().get(3).toString());
				// System.out.println(ra2);
				if (ra2 == null)
					ra2 = Float.parseFloat(function.getParameters().getExpressions().get(2).toString());
				if (dec1 == null)
					dec1 = Float.parseFloat(function.getParameters().getExpressions().get(1).toString());
				String subQueryR = "SELECT * FROM " + ObjectTable + " as " + alias + " where " + alias + ".ra between "
						+ ra1 + " and " + ra2 + " and " + alias + ".dec between " + dec1 + " and " + dec2;
				try { // Photoflags table never used in SDSS log, therefore no
						// need to check for multiple occurrences
					stack.push(parent);
					Statement stmt = CCJSqlParserUtil.parse(subQueryR);
					Select selectStatement = (Select) stmt;
					SubSelect subSelect = new SubSelect();
					subSelect.setSelectBody((PlainSelect) selectStatement.getSelectBody());
					subSelect.accept((ExpressionVisitor) this);
					if (right != null)
						right.accept(this);
					else
						stack.pop();
				} catch (JSQLParserException e) {
					// System.out.println("Could not parse Subquery from
					// function: "+function);
					// e.printStackTrace();
				}
				break;
			// case "flambdafromeq":
			// //http://www.sdss3.org/dr10/software/idlutils_doc.php#EQ2CSURVEY
			// -> Appears ONLY in SELECT as flambdafromeq(ra,dec), no numbers
			// given
			// Calculation: lambda= r2d*asin(cos(dec*d2r)*sin((ra-racen)*d2r))
			// where r2d= 180.0D/(!DPI) and d2r= 1.D/r2d and racen = RA of
			// center
			// break;
			case "fphotoflags": // fPhotoFlags('[...]') where [...] is some
								// String can always be rewritten as "SELECT
								// value FROM PhotoFlags WHERE name = '[...]'"
				String subQuery = "SELECT \"PhotoFlags.value\" FROM PhotoFlags WHERE PhotoFlags.name = "
						+ function.getParameters().getExpressions().get(0).toString(); // need
																						// "
																						// around
																						// value
																						// since
																						// it
																						// else
																						// could
																						// not
																						// be
																						// parsed
																						// (reserved
																						// expression)
				if (fPhotoFlags == 0)
					try { // Photoflags table never used in SDSS log, therefore
							// no need to check for multiple occurrences
							// //Problem: function used multiple times in some
							// queries
						stack.push(parent);
						Statement stmt = CCJSqlParserUtil.parse(subQuery);
						Select selectStatement = (Select) stmt;
						SubSelect subSelect = new SubSelect();
						subSelect.setSelectBody((PlainSelect) selectStatement.getSelectBody());
						fPhotoFlags++;
						subSelect.accept((ExpressionVisitor) this);
						right.accept(this);
					} catch (JSQLParserException e) {
						// System.out.println("Could not parse Subquery from
						// function: "+function);
						// e.printStackTrace();
					}
				else {
					try {
						subQuery = "SELECT \"PhotoFlags" + fPhotoFlags + ".value\" FROM PhotoFlags as PhotoFlags"
								+ fPhotoFlags + " WHERE PhotoFlags" + fPhotoFlags + ".name = "
								+ function.getParameters().getExpressions().get(0).toString(); // need
																								// "
																								// around
																								// value
																								// since
																								// it
																								// else
																								// could
																								// not
																								// be
																								// parsed
																								// (reserved
																								// expression)

						fPhotoFlags++;
						stack.push(parent);
						Statement stmt = CCJSqlParserUtil.parse(subQuery);
						Select selectStatement = (Select) stmt;
						SubSelect subSelect = new SubSelect();
						subSelect.setSelectBody((PlainSelect) selectStatement.getSelectBody());
						subSelect.accept((ExpressionVisitor) this);
						stack.pop();
						stack.pop();
						if (parent != null) {
							stack.push(new BooleanValue(true));
							stack.push(new BooleanValue(true));
							if (right != null) {
								right.accept(this);
								stack.pop();
							}
						}
						stack.push(new BooleanValue(true));
					} catch (Exception e) {
						// System.out.println("Could not parse Subquery from
						// function: "+function);
						// e.printStackTrace();
					}
				}
				break;
			case "fphotoflagsn": // returns the concatenated String of all Flags
									// corresponding to a value. These are all
									// taken from PhotoFlags table.
									// As we would need the actual values from
									// the PhotoFlags Table to calculate all the
									// contained Values and accessed rows
									// we will add the PhotoFlags table to our
									// list of accessed tables and access all
									// rows as for each row a state is allowed
									// where this tuple might influence the
									// result
				String subQueryN = "SELECT * FROM PhotoFlags";
				try { // Photoflags table never used in SDSS log, therefore no
						// need to check for multiple occurrences
					stack.push(new BooleanValue(true));
					Statement stmt = CCJSqlParserUtil.parse(subQueryN);
					Select selectStatement = (Select) stmt;
					SubSelect subSelect = new SubSelect();
					subSelect.setSelectBody((PlainSelect) selectStatement.getSelectBody());
					subSelect.accept((ExpressionVisitor) this);
					if (right != null) {
						right.accept(this);
						stack.pop();
					}
					stack.push(new BooleanValue(true));
				} catch (JSQLParserException e) {
					// System.out.println("Could not parse Subquery from
					// function: "+function);
					// e.printStackTrace();
				}
				break;
			case "fphototypen": // Returns the PhotoType name, indexed by value
								// (3-> Galaxy, 6-> Star,...)
								// Can be rewritten as "select name from
								// PhotoType where value = [...]" where [...] is
								// the Parameter of the function
				String subQueryT = "SELECT name FROM PhotoType WHERE \"value\" = "
						+ function.getParameters().getExpressions().get(0).toString();
				try { // PhotoType table never used in SDSS log, therefore no
						// need to check for multiple occurrences
					stack.push(parent);
					Statement stmt = CCJSqlParserUtil.parse(subQueryT);
					Select selectStatement = (Select) stmt;
					SubSelect subSelect = new SubSelect();
					subSelect.setSelectBody((PlainSelect) selectStatement.getSelectBody());
					subSelect.accept((ExpressionVisitor) this);
					if (right != null)
						right.accept(this);
					else
						stack.pop();
				} catch (JSQLParserException e) {
					// System.out.println("Could not parse Subquery from
					// function: "+function);
					// e.printStackTrace();
				}
				break;
			default:
				if (parent != null) {
					stack.push(null);
					stack.push(null);
					right.accept(this); // to preserve possible following
										// subqueries
					stack.pop();
				}
				stack.push(null);
			}
			// Aggregate functions:
			// } else if(name.equalsIgnoreCase("sum")) { //Always true as long
			// as no other constraints on the column , change if you want to
			// catch more types later
			// } else if(name.equalsIgnoreCase("avg")) { //Always true as long
			// as no other constraints on the column , change if you want to
			// catch more types later
			// } else if(name.equalsIgnoreCase("count")) { //Always true as long
			// as no other constraints on the column , change if you want to
			// catch more types later
		} else if (name.equalsIgnoreCase("min")) {// if the aggregate function
													// is a MIN, we have to
													// handle the different
													// cases
			if (parent instanceof MinorThan || parent instanceof MinorThanEquals || parent instanceof NotEqualsTo) { // here
																														// it
																														// becomes
																														// TRUE
				stack.push(new BooleanValue(true));
				stack.push(new BooleanValue(true));
				right.accept(this); // to preserve possible following subqueries
				stack.pop();
				stack.push(new BooleanValue(true));
			} else if (parent instanceof EqualsTo || parent instanceof GreaterThanEquals) {// here
																							// it
																							// becomes
																							// parameter
																							// >=
																							// x
				stack.push(new GreaterThanEquals());
				stack.push(function.getParameters().getExpressions().get(0));
				stack.push(right);
			} else if (parent instanceof GreaterThan) {// here it becomes
														// parameter > x
				stack.push(new GreaterThan());
				stack.push(function.getParameters().getExpressions().get(0));
				stack.push(right);
			}
		} else if (name.equalsIgnoreCase("max")) {
			if (parent instanceof GreaterThan || parent instanceof GreaterThanEquals || parent instanceof NotEqualsTo) {// TRUE
				stack.push(new BooleanValue(true));
				stack.push(new BooleanValue(true));
				right.accept(this);
				stack.pop();
				stack.push(new BooleanValue(true));
			} else if (parent instanceof EqualsTo || parent instanceof MinorThanEquals) {// here
																							// it
																							// becomes
																							// parameter
																							// <=
																							// x
				stack.push(new MinorThanEquals());
				stack.push(function.getParameters().getExpressions().get(0));
				stack.push(right);
			} else if (parent instanceof MinorThan) {// here it becomes
														// parameter < x
				stack.push(new MinorThan());
				stack.push(function.getParameters().getExpressions().get(0));
				stack.push(right);
			}
		} else {
			if (parent != null) {
				stack.push(new BooleanValue(true));
				stack.push(new BooleanValue(true));
				right.accept(this);
				stack.pop();
			}
			stack.push(new BooleanValue(true));
		}
	}

	@Override
	public void visit(InverseExpression inverseExpression) { // is a negative
																// value, is
																// just kept
																// as-is
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
	public void visit(Parenthesis parenthesis) {// we proceed to the element
												// inside the parenthesis and
												// remove the parenthesis if the
												// parent is an AND and the
												// element inside is not an OR
												// expression
		stack.push(parenthesis);
		Expression tempExpression = parenthesis.getExpression();
		if (parenthesis.isNot())
			((BinaryExpression) tempExpression).toggleNot();
		tempExpression.accept(this);
		tempExpression = stack.pop();
		parenthesis = (Parenthesis) stack.pop();
		if (stack.isEmpty())
			stack.push(tempExpression);
		else if (((stack.peek() instanceof AndExpression) && !(tempExpression instanceof OrExpression))
				|| tempExpression == null || tempExpression instanceof BooleanValue)
			stack.push(tempExpression);
		else {
			parenthesis.setNot(false);
			parenthesis.setExpression(tempExpression);
			stack.push(parenthesis);
		}
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
		if (andExpression.isNot()) {// process the NOT
			OrExpression orExpression = new OrExpression(null, null);
			orExpression.setNot();
			stack.push(orExpression);
		} else {
			stack.push(new AndExpression(null, null));
		}
		visitBinaryExpression(andExpression);

	}

	@Override
	public void visit(OrExpression orExpression) {
		if (orExpression.isNot()) {// process the NOT
			AndExpression andExpression = new AndExpression(null, null);
			andExpression.setNot();
			stack.push(andExpression);
		} else {
			stack.push(new OrExpression(null, null));
		}
		visitBinaryExpression(orExpression);
	}

	@Override
	public void visit(Between between) {// For the BETWEEN we need two new
										// predicates combined by AND or OR
		if (between.isNot()) {// greater than and minorthan, connected by OR
			Parenthesis parenthesis = new Parenthesis();
			GreaterThan greaterThan = new GreaterThan();
			greaterThan.setLeftExpression(between.getLeftExpression());
			greaterThan.setRightExpression(between.getBetweenExpressionEnd());
			MinorThan minorThan = new MinorThan();
			minorThan.setLeftExpression(between.getLeftExpression());
			minorThan.setRightExpression(between.getBetweenExpressionStart());

			OrExpression binaryExpression = new OrExpression(greaterThan, minorThan);
			parenthesis.setExpression(binaryExpression);
			stack.push(parenthesis);
		} else {// greaterthanequals and minorthanequals, connected by AND
			GreaterThanEquals greaterThanEquals = new GreaterThanEquals();
			greaterThanEquals.setLeftExpression(between.getLeftExpression());
			greaterThanEquals.setRightExpression(between.getBetweenExpressionStart());
			MinorThanEquals minorThanEquals = new MinorThanEquals();
			minorThanEquals.setLeftExpression(between.getLeftExpression());
			minorThanEquals.setRightExpression(between.getBetweenExpressionEnd());

			AndExpression binaryExpression = new AndExpression(greaterThanEquals, minorThanEquals);
			stack.push(binaryExpression);
		}
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		if (equalsTo.isNot())
			stack.push(new NotEqualsTo());// if NOT it becomes notequalsto
		else
			stack.push(new EqualsTo());
		visitBinaryExpression(equalsTo);
	}

	@Override
	public void visit(GreaterThan greaterThan) {
		// System.out.println(greaterThan);
		if (greaterThan.isNot())
			stack.push(new MinorThanEquals());// if NOT becomes minorthanequals
		else
			stack.push(new GreaterThan());
		visitBinaryExpression(greaterThan);
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		if (greaterThanEquals.isNot())
			stack.push(new MinorThan());// if NOT becomes minorthan
		else
			stack.push(new GreaterThanEquals());
		visitBinaryExpression(greaterThanEquals);
	}

	@Override
	public void visit(InExpression inExpression) { // two types, list or
													// subquery, is list in sdss
													// server often? NEVER since
													// it is no MS SQL syntax.
													// but anyway we catch it
		// need to extract the access area from the subquery and transform the
		// rest according to our theory
		SQLParser parser = new SQLParser();
		if (inExpression.getRightItemsList() instanceof SubSelect) {// Subquery
			try {// get access area of subquery
				PlainSelect tempSubSelect = (PlainSelect) ((SubSelect) inExpression.getRightItemsList())
						.getSelectBody();
				AccessArea tempAccessArea = parser.parseStmt(tempSubSelect.toString());
				BinaryExpression tempEqualsTo;
				if (inExpression.isNot()) {// if NOT IN its a notquealsto
					tempEqualsTo = new NotEqualsTo();
				} else {
					tempEqualsTo = new EqualsTo();
				} // left side of the expression stays the sime, right side is
					// the attribute from the SELECT clause of the subquery
				tempEqualsTo.setLeftExpression(inExpression.getLeftExpression());
				tempEqualsTo.setRightExpression(
						(Expression) ((SelectExpressionItem) tempSubSelect.getSelectItems().get(0)).getExpression());

				// Merge the access areas from and where part
				if (tempAccessArea.getFrom() != null) {
					List<FromItem> tempFromItemList = accessArea.getFrom();
					tempFromItemList.addAll(tempAccessArea.getFrom());
					accessArea.setFrom(tempFromItemList);
				}
				if (tempAccessArea.getWhere() != null) {
					AndExpression tempAndExpression = new AndExpression(null, null);
					tempAndExpression.setLeftExpression(tempEqualsTo);
					tempAndExpression.setRightExpression(tempAccessArea.getWhere());
					stack.push(tempAndExpression);
				} else
					stack.push(tempEqualsTo);
			} catch (JSQLParserException e) {
				// System.out.println("Error: Could not parse SubSelect of IN
				// Statement:" + inExpression);
				// e.printStackTrace();
			}
		} else {
			if (inExpression.getRightItemsList() != null) {// If right side is a
															// list... create
															// the predicates
															// and connect by
															// AND
				ExpressionList rightItemsList = (ExpressionList) inExpression.getRightItemsList();
				List<Expression> expressions = rightItemsList.getExpressions();

				ListIterator<Expression> it = expressions.listIterator();
				Expression expression = it.next();
				BinaryExpression tempEqualsTo;
				if (inExpression.isNot()) {// if not always notequalsto
					tempEqualsTo = new NotEqualsTo();
				} else {
					tempEqualsTo = new EqualsTo();
				}
				tempEqualsTo.setLeftExpression(inExpression.getLeftExpression());
				tempEqualsTo.setRightExpression(expression);
				if (it.hasNext()) {// iterate through the list
					expression = it.next();
					BinaryExpression tempBinExpression;
					BinaryExpression tempEqualsTo2;
					if (inExpression.isNot()) {
						tempEqualsTo2 = new NotEqualsTo();
						tempBinExpression = new AndExpression(null, null);
					} else {
						tempEqualsTo2 = new EqualsTo();
						tempBinExpression = new OrExpression(null, null);
					}
					tempBinExpression.setLeftExpression(tempEqualsTo);
					tempEqualsTo2.setLeftExpression(inExpression.getLeftExpression());
					tempEqualsTo2.setRightExpression(expression);
					tempBinExpression.setRightExpression(tempEqualsTo2);
					while (it.hasNext()) {
						expression = it.next();
						BinaryExpression tempBinExpression2;
						BinaryExpression tempEqualsTo3;
						if (inExpression.isNot()) {
							tempEqualsTo3 = new NotEqualsTo();
							tempBinExpression2 = new AndExpression(null, null);
						} else {
							tempEqualsTo3 = new EqualsTo();
							tempBinExpression2 = new OrExpression(null, null);
						}
						tempBinExpression2.setLeftExpression(tempBinExpression);
						tempEqualsTo3.setLeftExpression(inExpression.getLeftExpression());
						tempEqualsTo3.setRightExpression(expression);
						tempBinExpression2.setRightExpression(tempEqualsTo3);
						tempBinExpression = tempBinExpression2;
					}
					Parenthesis parenthesis = new Parenthesis(tempBinExpression);// parethisis
																					// around
																					// it
																					// all
					stack.push(parenthesis);
				} else
					stack.push(tempEqualsTo);
			}
		}
	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
		stack.push(isNullExpression);
	}

	@Override
	public void visit(LikeExpression likeExpression) {
		LikeExpression tempLike = new LikeExpression();
		if (likeExpression.isNot())
			tempLike.setNot(true);
		if (likeExpression.getEscape() != null)
			tempLike.setEscape(likeExpression.getEscape());
		stack.push(tempLike);
		visitBinaryExpression(likeExpression);
	}

	@Override
	public void visit(MinorThan minorThan) {
		if (minorThan.isNot())
			stack.push(new GreaterThanEquals());// if not becomes
												// greaterthanequals
		else
			stack.push(new MinorThan());
		visitBinaryExpression(minorThan);
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		if (minorThanEquals.isNot())
			stack.push(new GreaterThan()); // if NOT becomes greaterthan
		else
			stack.push(new MinorThanEquals());
		visitBinaryExpression(minorThanEquals);
	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		if (notEqualsTo.isNot())
			stack.push(new EqualsTo()); // if NOT becomes equalsto
		else
			stack.push(new NotEqualsTo());
		visitBinaryExpression(notEqualsTo);
	}

	@Override
	public void visit(Column tableColumn) {
		stack.push(tableColumn);
	}

	@Override
	public void visit(CaseExpression caseExpression) {// only occurs mostly in
														// select clause in our
														// SDSS share, now
														// information about
														// cases
		stack.push(caseExpression);
	}

	@Override
	public void visit(WhenClause whenClause) { // 27 in SDSS: mostly in select
												// part, nearly always as part
												// of case expression
		stack.push(whenClause);
	}

	@Override
	public void visit(ExistsExpression existsExpression) {
		if (existsExpression.isNot()) {// NOTEXISTS is always transformed to
										// TRUE
			if (existsExpression.getRightExpression() instanceof SubSelect) {
				stack.push(new BooleanValue(true));
			} else
				stack.push(new BooleanValue(true));
		} else {
			if (existsExpression.getRightExpression() instanceof SubSelect) {// like
																				// with
																				// IN
																				// expression.
																				// extract
																				// access
																				// area
																				// of
																				// subquery
																				// and
																				// merge
																				// the
																				// access
																				// areas
				// here of course the EXISTS is not replaced by any new
				// predicate except for those in the subquery
				SQLParser parser = new SQLParser();
				PlainSelect tempSubSelect = (PlainSelect) ((SubSelect) existsExpression.getRightExpression())
						.getSelectBody();
				try {
					AccessArea tempAccessArea = parser.parseStmt(tempSubSelect.toString());
					if (tempAccessArea.getFrom() != null) {
						List<FromItem> tempFromItemList = accessArea.getFrom();
						tempFromItemList.addAll(tempAccessArea.getFrom());
						accessArea.setFrom(tempFromItemList);
					}
					if (tempAccessArea.getWhere() != null) {
						stack.push(tempAccessArea.getWhere());
					} else
						stack.push(new BooleanValue(true));
				} catch (JSQLParserException e) {
					// System.out.println("Error: Could not parse SubSelect of
					// EXISTS Statement:" + existsExpression);
					// e.printStackTrace();
				}
			} else
				stack.push(new BooleanValue(true));
		}
	}

	@Override
	public void visit(AllComparisonExpression allComparisonExpression) {// ALL
																		// is
																		// converted
																		// to
																		// ANY
																		// and
																		// then
																		// we go
																		// on
																		// there
		Expression left = stack.pop();
		Expression parent = stack.pop();
		Expression tempExpression;
		if (parent instanceof MinorThanEquals)
			tempExpression = new GreaterThan();
		else if (parent instanceof MinorThan)
			tempExpression = new GreaterThanEquals();
		else if (parent instanceof GreaterThanEquals)
			tempExpression = new MinorThan();
		else if (parent instanceof GreaterThan)
			tempExpression = new MinorThanEquals();
		else if (parent instanceof EqualsTo)
			tempExpression = new NotEqualsTo();
		else if (parent instanceof NotEqualsTo)
			tempExpression = new EqualsTo();
		else
			tempExpression = parent;
		if (((BinaryExpression) parent).isNot())
			((BinaryExpression) tempExpression).setNot(false);// catch NOT
		else
			((BinaryExpression) tempExpression).setNot(true);
		AnyComparisonExpression tempAnyComparisonExpression = new AnyComparisonExpression(
				allComparisonExpression.getSubSelect());

		stack.push(tempExpression);
		stack.push(left);

		tempAnyComparisonExpression.accept(this);
	}

	@Override
	public void visit(AnyComparisonExpression anyComparisonExpression) {// we
																		// get
																		// the
																		// access
																		// area
																		// of
																		// the
																		// subquery
																		// and
																		// merge
																		// it
																		// with
																		// the
																		// outer
																		// one
		SQLParser parser = new SQLParser();
		if (anyComparisonExpression.getSubSelect() instanceof SubSelect) {
			try {
				PlainSelect tempSubSelect = (PlainSelect) (anyComparisonExpression.getSubSelect()).getSelectBody();
				AccessArea tempAccessArea = parser.parseStmt(tempSubSelect.toString());
				Expression tempExpression = (Expression) ((SelectExpressionItem) tempSubSelect.getSelectItems().get(0))
						.getExpression();

				if (tempAccessArea.getFrom() != null) {
					List<FromItem> tempFromItemList = accessArea.getFrom();
					tempFromItemList.addAll(tempAccessArea.getFrom());
					accessArea.setFrom(tempFromItemList);
				}

				if (tempAccessArea.getWhere() != null && !(tempAccessArea.getWhere() instanceof BooleanValue)) {
					AndExpression tempAndExpression = new AndExpression(null, null);
					tempAndExpression.setLeftExpression(tempExpression);
					tempAndExpression.setRightExpression(tempAccessArea.getWhere());
					stack.push(tempAndExpression);
				} else
					stack.push(tempExpression);
			} catch (JSQLParserException e) {
				// System.out.println("Error: Could not parse SubSelect of ANY
				// Statement:" + anyComparisonExpression);
				// e.printStackTrace();
			}
		}
	}

	@Override
	public void visit(Concat concat) {
		stack.push(new Concat());
		visitBinaryExpression(concat);
	}

	@Override
	public void visit(Matches matches) { // only seems to occur in comments and
											// names
		stack.push(matches);
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
		stack.push(cast.getLeftExpression());
	}

	@Override
	public void visit(Modulo modulo) {
		stack.push(new Modulo());
		visitBinaryExpression(modulo);
	}

	@Override
	public void visit(AnalyticExpression aexpr) {// should not occur here?
		// System.out.println("Test");
		stack.push(aexpr);
	}

	@Override
	public void visit(ExtractExpression eexpr) { // occurs zero times
	}

	@Override
	public void visit(IntervalExpression iexpr) { // Not in MS SQL?
	}

	@Override
	public void visit(Table tableName) { // Not necessary here as TableName is
											// no Expression
	}

	@Override
	public void visit(SubSelect subSelect) { // should be handled already
												// somewhere else (EXISTS, IN,
												// ALL, ANY,...) but could occur
												// in WHERE part anyway
		// we extract access area and save in subwhere. merge it with the outer
		// access area later. at the current spot, the subquery is replaced by
		// the attribute of the SELECT clause of the subquery
		SQLParser parser = new SQLParser();
		try {
			PlainSelect tempSubSelect = (PlainSelect) subSelect.getSelectBody();
			AccessArea tempAccessArea = parser.parseStmt(tempSubSelect.toString());
			Expression tempExpression;
			if ((tempSubSelect.getSelectItems().get(0)) instanceof AllColumns) {
				tempExpression = new BooleanValue(true);
			} else {
				tempExpression = (Expression) ((SelectExpressionItem) tempSubSelect.getSelectItems().get(0))
						.getExpression();
			}

			if (tempAccessArea.getFrom() != null) {
				List<FromItem> tempFromItemList = accessArea.getFrom();
				tempFromItemList.addAll(tempAccessArea.getFrom());
				accessArea.setFrom(tempFromItemList);
			}

			if (tempAccessArea.getWhere() != null) {
				if (subWhere != null) {
					AndExpression tempAndExpression = new AndExpression(null, null);
					if (subWhere instanceof Parenthesis)
						tempAndExpression.setLeftExpression(subWhere);
					else {
						Parenthesis tempParenthesis = new Parenthesis();
						tempParenthesis.setExpression(subWhere);
						tempAndExpression.setLeftExpression(tempParenthesis);
					}
					tempAndExpression.setRightExpression(tempAccessArea.getWhere());
					subWhere = tempAndExpression;
				} else
					subWhere = tempAccessArea.getWhere();
			}
			stack.push(tempExpression);
		} catch (JSQLParserException e) {
			// System.out.println("Error: Could not parse SubSelect of
			// SubSelect:" + subSelect);
			// e.printStackTrace();
		}
	}

	@Override
	public void visit(SubJoin subjoin) { // should already be handled elsewhere
	}

	@Override
	public void visit(LateralSubSelect lateralSubSelect) { // should already be
															// handled elsewhere
	}

	@Override
	public void visit(ValuesList valuesList) { // should already be handled
												// elsewhere
	}

	@Override
	public void visit(AllColumns allColumns) { // should already be handled
												// elsewhere
	}

	@Override
	public void visit(AllTableColumns allTableColumns) { // should already be
															// handled elsewhere
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) { // should
																	// already
																	// be
																	// handled
																	// elsewhere
		// selectExpressionItem.getExpression().accept(this);
	}

	public ExpressionVisitor getExpressionVisitor() {
		return expressionVisitor;
	}

	public void setExpressionVisitor(ExpressionVisitor visitor) {
		expressionVisitor = visitor;
	}

	@Override
	public void visit(PlainSelect plainSelect) { // should already be handled
													// elsewhere

	}

	@Override
	public void visit(SetOperationList setOpList) { // should already be handled
													// elsewhere
	}

	@Override
	public void visit(WithItem withItem) { // occurs only in FROM part, only
											// rarely there and if handled it
											// will be there, but there most are
											// errors
	}

	@Override
	public void visit(BooleanValue booleanValue) {
		stack.push(booleanValue);
	}

	@Override
	public void visit(FunctionItem functionItem) { // should only occur in
													// subquery and these are
													// already handled
		functionItem.getFunction().accept(this);
	}
}
