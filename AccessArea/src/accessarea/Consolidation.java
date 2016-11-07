/* Written by FlorianB for SQLQueryLogTransformer:
 * This file compares predicates of different clauses and checks for conflicts or overlapping of intervals
 * we collect all predicates on the same attribute (including any table name and alias of course) of any clause with only a single literal first
 * then we check them for conflicts and confine their intervals.
 * afterwards we check for conflicts or overlapping intervals in any other clause with multiple literals
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
import net.sf.jsqlparser.statement.select.SubSelect;

public class Consolidation implements ExpressionVisitor {

	private ExpressionVisitor expressionVisitor;
	private Stack<Expression> stack = new Stack<Expression>();
	private Integer run = 1;
	private TreeMap<String, List<Expression>> tm = new TreeMap<String, List<Expression>>();
	private TreeMap<String, List<Expression>> tmOR = new TreeMap<String, List<Expression>>();
	private List<String> stringListOR = new ArrayList<String>();

	public Expression consolidate(Expression where) {
		for (run = 1; run <= 2; run++) {
			if (run.equals(2)) { // after we got the predicates from single
									// clauses we consolidate them
				tm = consolidateTreeMap(tm);
				if (tm.containsKey("FALSE"))
					return new BooleanValue(false);// IF our treemap contains
													// the FALSE key it means a
													// conflict was detected.
													// Query could not return
													// any tuple, access area is
													// empty
			}
			if (where != null) { // get all predicates from single clauses in
									// run 1, compare new refined predicates
									// with all literals in larger clauses in
									// run 2
				where.accept(this);// in this step in run 1 we also remove all
									// single clauses. they are added after this
									// while loop
				where = stack.pop();
			}
		}
		while (!tm.isEmpty()) {// Create AND expressions from the refined
								// clauses and connect to the rest of the
								// constraints
			List<Expression> expressionList = tm.firstEntry().getValue();
			while (!expressionList.isEmpty()) {
				if (where != null) {
					AndExpression andExpression = new AndExpression(null, null);
					andExpression.setLeftExpression(where);
					andExpression.setRightExpression(expressionList.get(0));
					where = andExpression;
				} else
					where = expressionList.get(0);
				expressionList.remove(0);
			}
			tm.remove(tm.firstEntry().getKey());
		}
		return where;
	}

	// this function compares each predicate in the treemap to each other
	// predicate in the treemap that uses the same attribute on the left hand
	// side
	// this represents the table of possible combinations of predicates from the
	// thesis.
	// for every comparison we check if we keep one or both predicates
	public TreeMap<String, List<Expression>> consolidateTreeMap(TreeMap<String, List<Expression>> treeMap) {
		TreeMap<String, List<Expression>> treeMapNew = new TreeMap<String, List<Expression>>();
		while (!treeMap.isEmpty()) {// We construct our new consolidated tree
									// map
			String column = treeMap.firstEntry().getKey();
			List<Expression> expressionList = treeMap.firstEntry().getValue();
			List<Expression> expressionListNew = new ArrayList<Expression>();// our
																				// new
																				// consolidated
																				// expressions
			List<LikeExpression> expressionListLike = new ArrayList<LikeExpression>();// likes
																						// are
																						// kept
																						// in
																						// most
																						// cases
																						// additionally
			List<Expression> expressionListSpecial = new ArrayList<Expression>();// further
																					// expressions
																					// are
																					// also
																					// kept
																					// additionally
			List<NotEqualsTo> expressionListNotEqualsTo = new ArrayList<NotEqualsTo>(); // not
																						// equals
																						// to
																						// are
																						// also
																						// kept
																						// in
																						// most
																						// cases

			if (expressionList.size() > 1) {
				for (int j = 0; j < expressionList.size(); j++) {
					Expression expression1 = expressionList.get(j);
					Expression expression2 = null;
					Expression expression3 = null;
					Boolean checked = false;
					Boolean comparation = false;
					Boolean expression3Boolean = false;
					String string1 = null;
					String string2 = null;

					// we always compare one predicate with another one.
					// The next few rather short if-clauses are there to check
					// if the right hand side of a predicate is another binary
					// expression (like an addition), we do not check these and
					// keep them anyway
					if (expression1 instanceof EqualsTo) {
						if (((EqualsTo) expression1).getRightExpression() instanceof BinaryExpression
								|| ((EqualsTo) expression1).getRightExpression() instanceof Column) {
							for (int k = 0; k < expressionListSpecial.size(); k++) {
								if (expressionListSpecial.get(k).toString().equals(expression1.toString())) {
									checked = true;
								}
							}
							if (!checked)
								expressionListSpecial.add(expression1);
							expression1 = null;
							checked = false;
						}
					} else if (expression1 instanceof NotEqualsTo) {
						if (((NotEqualsTo) expression1).getRightExpression() instanceof BinaryExpression
								|| ((NotEqualsTo) expression1).getRightExpression() instanceof Column) {
							for (int k = 0; k < expressionListSpecial.size(); k++) {
								if (expressionListSpecial.get(k).toString().equals(expression1.toString())) {
									checked = true;
								}
							}
							if (!checked)
								expressionListSpecial.add(expression1);
							expression1 = null;
							checked = false;
						}
					} else if (expression1 instanceof LikeExpression) {
						if (((LikeExpression) expression1).getRightExpression() instanceof BinaryExpression
								|| ((LikeExpression) expression1).getRightExpression() instanceof Column) {
							for (int k = 0; k < expressionListSpecial.size(); k++) {
								if (expressionListSpecial.get(k).toString().equals(expression1.toString())) {
									checked = true;
								}
							}
							if (!checked)
								expressionListSpecial.add(expression1);
							expression1 = null;
							checked = false;
						}
					} else if (expression1 instanceof MinorThan) {
						if (((MinorThan) expression1).getRightExpression() instanceof BinaryExpression
								|| ((MinorThan) expression1).getRightExpression() instanceof Column) {
							for (int k = 0; k < expressionListSpecial.size(); k++) {
								if (expressionListSpecial.get(k).toString().equals(expression1.toString())) {
									checked = true;
								}
							}
							if (!checked)
								expressionListSpecial.add(expression1);
							expression1 = null;
							checked = false;
						}
					} else if (expression1 instanceof MinorThanEquals) {
						if (((MinorThanEquals) expression1).getRightExpression() instanceof BinaryExpression
								|| ((MinorThanEquals) expression1).getRightExpression() instanceof Column) {
							for (int k = 0; k < expressionListSpecial.size(); k++) {
								if (expressionListSpecial.get(k).toString().equals(expression1.toString())) {
									checked = true;
								}
							}
							if (!checked)
								expressionListSpecial.add(expression1);
							expression1 = null;
							checked = false;
						}
					} else if (expression1 instanceof GreaterThan) {
						if (((GreaterThan) expression1).getRightExpression() instanceof BinaryExpression
								|| ((GreaterThan) expression1).getRightExpression() instanceof Column) {
							for (int k = 0; k < expressionListSpecial.size(); k++) {
								if (expressionListSpecial.get(k).toString().equals(expression1.toString())) {
									checked = true;
								}
							}
							if (!checked)
								expressionListSpecial.add(expression1);
							expression1 = null;
							checked = false;
						}
					} else if (expression1 instanceof GreaterThanEquals) {
						if (((GreaterThanEquals) expression1).getRightExpression() instanceof BinaryExpression
								|| ((GreaterThanEquals) expression1).getRightExpression() instanceof Column) {
							for (int k = 0; k < expressionListSpecial.size(); k++) {
								if (expressionListSpecial.get(k).toString().equals(expression1.toString())) {
									checked = true;
								}
							}
							if (!checked)
								expressionListSpecial.add(expression1);
							expression1 = null;
							checked = false;
						}
					}
					if (!expressionListNew.isEmpty() && expression1 != null) {
						expression2 = expressionListNew.get(0);// here we get
																// the second
																// expression
																// and start
																// comparing
						if (expressionListNew.size() > 1) {
							expression3 = expressionListNew.get(1);
							if (expression1.toString().equals(expression3.toString())) {
								expression3Boolean = true;
							}
						}
						if (!(expression1.toString().equals(expression2.toString())) && !expression3Boolean) {
							if (expression1 instanceof EqualsTo) {// Compare
																	// equals to
																	// to all
																	// others,
																	// first
																	// another
																	// equalsto
																	// and so on
								if (!expressionListNotEqualsTo.isEmpty()) {// If
																			// there
																			// already
																			// are
																			// items
																			// in
																			// our
																			// new
																			// list
																			// of
																			// notequalsto
																			// expressions
																			// we
																			// have
																			// to
																			// check
																			// if
																			// there
																			// is
																			// already
																			// a
																			// conflict
									ListIterator<NotEqualsTo> it = expressionListNotEqualsTo.listIterator();
									while (it.hasNext()) {
										string1 = ((EqualsTo) expression1).getRightExpression().toString();
										string2 = ((NotEqualsTo) expression2).getLeftExpression().toString();
										if (string1.equals(string2)) {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
									expressionListNotEqualsTo.clear();
								}
								if (expression2 instanceof EqualsTo) {// Both
																		// are
																		// equalsto
																		// expressions
																		// ->
																		// FALSE
																		// if
																		// different
																		// right
																		// sides
									treeMapNew.put("FALSE", null);
									return treeMapNew;
								} else if (expression2 instanceof NotEqualsTo) {// equalsto
																				// vs.
																				// notequalsto
																				// ->
																				// if
																				// same
																				// right
																				// side
																				// it
																				// means
																				// FALSE,
																				// else
																				// keep
																				// both
									string1 = ((EqualsTo) expression1).getRightExpression().toString();
									string2 = ((NotEqualsTo) expression2).getLeftExpression().toString();
									if (string1.equals((string2))) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof IsNullExpression) {// equalsto
																						// vs.
																						// isnull
																						// and
																						// isnotnull
																						// ->
																						// FALSE
																						// if
																						// isnull,
																						// else
																						// keep
																						// only
																						// the
																						// equalsto
									if (!((IsNullExpression) expression2).isNot()) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} else
										expressionListNew.set(0, expression1);
									// The following cases are always the same
									// schema. compare two expressions and
									// decide which to keep
								} else if (expression2 instanceof LikeExpression) {
									if (!expressionListLike.contains((LikeExpression) expression2))
										expressionListLike.add((LikeExpression) expression2);
									expressionListNew.set(0, expression1);
								} else if (expression2 instanceof MinorThanEquals) {
									comparation = compareNumbers(expression1, expression2, ">");
									if (comparation != null) {
										if (comparation) {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										} else
											expressionListNew.set(0, expression1);
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof MinorThan) {
									comparation = compareNumbers(expression1, expression2, ">=");
									if (comparation != null) {
										if (comparation) {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										} else
											expressionListNew.set(0, expression1);
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof GreaterThan) {
									comparation = compareNumbers(expression1, expression2, "<=");
									if (comparation != null) {
										if (comparation) {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										} else
											expressionListNew.set(0, expression1);
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof GreaterThanEquals) {
									comparation = compareNumbers(expression1, expression2, "<");
									if (comparation != null) {
										if (comparation) {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										} else
											expressionListNew.set(0, expression1);
									} else
										expressionListNew.set(0, expression1);
								}
								// Now expression 1 is an notequalsto and is
								// compared to all other types
							} else if (expression1 instanceof NotEqualsTo) {
								if (expression2 instanceof EqualsTo) {
									string1 = ((EqualsTo) expression2).getRightExpression().toString();
									string2 = ((NotEqualsTo) expression1).getLeftExpression().toString();
									if (string1.equals((string2))) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} // else do nothing
								} else if (expression2 instanceof IsNullExpression) {
									if (!((IsNullExpression) expression2).isNot()) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof LikeExpression) {
									if (!expressionListLike.contains((LikeExpression) expression2))
										expressionListLike.add((LikeExpression) expression2);
									expressionListNew.set(0, expression1);
								} else {
									expressionListNotEqualsTo.add((NotEqualsTo) expression1);
								}
							} else if (expression1 instanceof MinorThan) { // Now
																			// expr1
																			// is
																			// a
																			// MinorThan.
																			// here
																			// we
																			// have
																			// to
																			// actually
																			// check
																			// which
																			// predicate
																			// has
																			// the
																			// larger
																			// or
																			// smaller
																			// value
								if (expression2 instanceof EqualsTo) {
									comparation = compareNumbers(expression1, expression2, "<=");
									if (comparation != null) {
										if (comparation) {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
								} else if (expression2 instanceof NotEqualsTo) {
									string1 = ((EqualsTo) expression1).getRightExpression().toString();
									string2 = ((NotEqualsTo) expression2).getLeftExpression().toString();
									if (string1.equals((string2))) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof IsNullExpression) {
									if (!((IsNullExpression) expression2).isNot()) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof LikeExpression) {
									if (!expressionListLike.contains((LikeExpression) expression2))
										expressionListLike.add((LikeExpression) expression2);
									expressionListNew.set(0, expression1);
								} else if (expression2 instanceof MinorThanEquals) {
									comparation = compareNumbers(expression1, expression2, "<=");
									if (comparation != null) {
										if (comparation) {
											expressionListNew.set(0, expression1);
										}
									}
								} else if (expression2 instanceof MinorThan) {
									comparation = compareNumbers(expression1, expression2, "<");
									if (comparation != null) {
										if (comparation) {
											expressionListNew.set(0, expression1);
										}
									}
								} else if (expression2 instanceof GreaterThan) {
									comparation = compareNumbers(expression1, expression2, ">");
									if (comparation != null) {
										if (comparation) {
											if (expression3 != null) {
												comparation = compareNumbers(expression1, expression3, "<=");
												if (comparation != null) {
													if (!comparation) {
														expression1 = expression3;
													}
												}
											}
											if (expressionListNew.size() < 2)
												expressionListNew.add(1, expression1);
											else
												expressionListNew.set(1, expression1);
										} else {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
								} else if (expression2 instanceof GreaterThanEquals) {
									comparation = compareNumbers(expression1, expression2, ">");
									if (comparation != null) {
										if (comparation) {
											if (expression3 != null) {
												comparation = compareNumbers(expression1, expression2, "<=");
												if (comparation != null) {
													if (comparation) {
														expression1 = expression3;
													}
												}
											}
											if (expressionListNew.size() < 2)
												expressionListNew.add(1, expression1);
											else
												expressionListNew.set(1, expression1);
										} else {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
								}
							} else if (expression1 instanceof MinorThanEquals) {
								if (expression2 instanceof EqualsTo) {
									comparation = compareNumbers(expression1, expression2, "<");
									if (comparation != null) {
										if (comparation) {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
								} else if (expression2 instanceof NotEqualsTo) {
									string1 = ((EqualsTo) expression1).getRightExpression().toString();
									string2 = ((NotEqualsTo) expression2).getLeftExpression().toString();
									if (string1.equals((string2))) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof IsNullExpression) {
									if (!((IsNullExpression) expression2).isNot()) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof LikeExpression) {
									if (!expressionListLike.contains((LikeExpression) expression2))
										expressionListLike.add((LikeExpression) expression2);
									expressionListNew.set(0, expression1);
								} else if (expression2 instanceof MinorThanEquals) {
									comparation = compareNumbers(expression1, expression2, "<");
									if (comparation != null) {
										if (comparation) {
											expressionListNew.set(0, expression1);
										}
									}
								} else if (expression2 instanceof MinorThan) {
									comparation = compareNumbers(expression1, expression2, "<");
									if (comparation != null) {
										if (comparation) {
											expressionListNew.set(0, expression1);
										}
									}
								} else if (expression2 instanceof GreaterThan) {
									comparation = compareNumbers(expression1, expression2, ">");
									if (comparation != null) {
										if (comparation) {
											if (expression3 != null) {
												comparation = compareNumbers(expression1, expression2, "<=");
												if (comparation != null) {
													if (!comparation) {
														expression1 = expression3;
													}
												}
											}
											if (expressionListNew.size() < 2)
												expressionListNew.add(1, expression1);
											else
												expressionListNew.set(1, expression1);
										} else {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
								} else if (expression2 instanceof GreaterThanEquals) {
									comparation = compareNumbers(expression1, expression2, ">=");
									if (comparation != null) {
										if (comparation) {
											if (expression3 != null) {
												comparation = compareNumbers(expression1, expression2, "<=");
												if (comparation != null) {
													if (!comparation) {
														expression1 = expression3;
													}
												}
											}
											if (expressionListNew.size() < 2)
												expressionListNew.add(1, expression1);
											else
												expressionListNew.set(1, expression1);
										} else {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
								}
							} else if (expression1 instanceof GreaterThan) {
								if (expression2 instanceof EqualsTo) {
									comparation = compareNumbers(expression1, expression2, ">=");
									if (comparation != null) {
										if (comparation) {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
								} else if (expression2 instanceof NotEqualsTo) {
									string1 = ((EqualsTo) expression1).getRightExpression().toString();
									string2 = ((NotEqualsTo) expression2).getLeftExpression().toString();
									if (string1.equals((string2))) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof IsNullExpression) {
									if (!((IsNullExpression) expression2).isNot()) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof LikeExpression) {
									if (!expressionListLike.contains((LikeExpression) expression2))
										expressionListLike.add((LikeExpression) expression2);
									expressionListNew.set(0, expression1);
								} else if (expression2 instanceof MinorThanEquals) {
									comparation = compareNumbers(expression1, expression2, "<");
									if (comparation != null) {
										if (comparation) {
											expressionListNew.set(0, expression1);
											if (expressionListNew.size() < 2)
												expressionListNew.add(1, expression2);
											else
												expressionListNew.set(1, expression2);
										} else {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
								} else if (expression2 instanceof MinorThan) {
									comparation = compareNumbers(expression1, expression2, "<");
									if (comparation != null) {
										if (comparation) {
											expressionListNew.set(0, expression1);
											if (expressionListNew.size() < 2)
												expressionListNew.add(1, expression2);
											else
												expressionListNew.set(1, expression2);
										} else {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
								} else if (expression2 instanceof GreaterThan) {
									comparation = compareNumbers(expression1, expression2, ">");
									if (comparation != null) {
										if (comparation) {
											expressionListNew.set(0, expression1);
										}
									}
								} else if (expression2 instanceof GreaterThanEquals) {
									comparation = compareNumbers(expression1, expression2, ">=");
									if (comparation != null) {
										if (comparation) {
											expressionListNew.set(0, expression1);
										}
									}
								}
							} else if (expression1 instanceof GreaterThanEquals) {
								if (expression2 instanceof EqualsTo) {
									comparation = compareNumbers(expression1, expression2, ">");
									if (comparation != null) {
										if (comparation) {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
								} else if (expression2 instanceof NotEqualsTo) {
									string1 = ((EqualsTo) expression1).getRightExpression().toString();
									string2 = ((NotEqualsTo) expression2).getLeftExpression().toString();
									if (string1.equals((string2))) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof IsNullExpression) {
									if (!((IsNullExpression) expression2).isNot()) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} else
										expressionListNew.set(0, expression1);
								} else if (expression2 instanceof LikeExpression) {
									if (!expressionListLike.contains((LikeExpression) expression2))
										expressionListLike.add((LikeExpression) expression2);
									expressionListNew.set(0, expression1);
								} else if (expression2 instanceof MinorThanEquals) {
									comparation = compareNumbers(expression1, expression2, "<=");
									if (comparation != null) {
										if (comparation) {
											expressionListNew.set(0, expression1);
											if (expressionListNew.size() < 2)
												expressionListNew.add(1, expression2);
											else
												expressionListNew.set(1, expression2);
										} else {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
								} else if (expression2 instanceof MinorThan) {
									comparation = compareNumbers(expression1, expression2, "<");
									if (comparation != null) {
										if (comparation) {
											expressionListNew.set(0, expression1);
											if (expressionListNew.size() < 2)
												expressionListNew.add(1, expression2);
											else
												expressionListNew.set(1, expression2);
										} else {
											treeMapNew.put("FALSE", null);
											return treeMapNew;
										}
									}
								} else if (expression2 instanceof GreaterThan) {
									comparation = compareNumbers(expression1, expression2, ">");
									if (comparation != null) {
										if (comparation) {
											expressionListNew.set(0, expression1);
										}
									}
								} else if (expression2 instanceof GreaterThanEquals) {
									comparation = compareNumbers(expression1, expression2, ">");
									if (comparation != null) {
										if (comparation) {
											expressionListNew.set(0, expression1);
										}
									}
								}
							} else if (expression1 instanceof IsNullExpression) {
								if (!((IsNullExpression) expression1).isNot()) {
									treeMapNew.put("FALSE", null);
									return treeMapNew;
								} else if (expression2 instanceof IsNullExpression
										&& !((IsNullExpression) expression2).isNot()) {
									treeMapNew.put("FALSE", null);
									return treeMapNew;
								}
							} else if (expression1 instanceof LikeExpression) {
								if (expression2 instanceof IsNullExpression) {
									if (!((IsNullExpression) expression1).isNot()) {
										treeMapNew.put("FALSE", null);
										return treeMapNew;
									} else {
										expressionListNew.set(0, expression1);
									}
								} else {
									if (!expressionListLike.contains((LikeExpression) expression1))
										expressionListLike.add((LikeExpression) expression1);
								}
							}
						}
					} else {
						if (expression1 != null)
							expressionListNew.add(expression1);
					}
				}
				expressionListNew.addAll(expressionListLike);
				expressionListNew.addAll(expressionListNotEqualsTo);
				expressionListNew.addAll(expressionListSpecial);
			} else {
				expressionListNew = expressionList;
			}
			treeMap.remove(column);
			treeMapNew.put(column, expressionListNew);
		}
		return treeMapNew;
	}

	// Here we consolidate the literals inside a clause, using a new TreeMap and
	// our consolidation methodo from above. if FALSE is returned than it can be
	// removed. else check of larger/smaller values
	public Expression consolidateOR(Column column, Expression expression) {

		List<Expression> expressionList = tmOR.get(column.getWholeColumnName());
		for (int j = 0; j < expressionList.size(); j++) {
			Expression expressionCompareTo = expressionList.get(j);
			if (expressionCompareTo instanceof IsNullExpression) {
				expression = null;
			} else if ((expression instanceof MinorThan || expression instanceof MinorThanEquals)
					&& (expressionCompareTo instanceof MinorThan || expressionCompareTo instanceof MinorThanEquals)) {
				if (expression instanceof MinorThan && expressionCompareTo instanceof MinorThanEquals) {
					Boolean comparation = compareNumbers(expression, expressionCompareTo, "<=");
					if (comparation != null) {
						if (comparation) {
							expression = null;
						}
					}
				} else {
					Boolean comparation = compareNumbers(expression, expressionCompareTo, "<");
					if (comparation != null) {
						if (comparation) {
							expression = null;
						}
					}
				}
			} else if ((expression instanceof GreaterThan || expression instanceof GreaterThanEquals)
					&& (expressionCompareTo instanceof GreaterThan
							|| expressionCompareTo instanceof GreaterThanEquals)) {
				if (expression instanceof GreaterThan && expressionCompareTo instanceof GreaterThanEquals) {
					Boolean comparation = compareNumbers(expression, expressionCompareTo, ">=");
					if (comparation != null) {
						if (comparation) {
							expression = null;
						}
					}
				} else {
					Boolean comparation = compareNumbers(expression, expressionCompareTo, ">");
					if (comparation != null) {
						if (comparation) {
							expression = null;
						}
					}
				}
			}
		}
		return expression;
	}

	// Whenever we reach a column it has to be on a left side due to the
	// visitbinaryexpression function. we then can add the parent predicate to
	// our tree map if we are in run 1
	public void processColumn(Column column) {
		Expression right = stack.pop();
		Expression parent = null;
		Expression expression = column;
		Boolean checked = false;
		if (right instanceof IsNullExpression) {// IsNULL has no right side
												// value
			parent = right;
			right = new NullValue();
		} else
			parent = stack.pop();
		if (run.equals(1) && (parent instanceof EqualsTo || parent instanceof NotEqualsTo || parent instanceof MinorThan
				|| parent instanceof MinorThanEquals || parent instanceof GreaterThan
				|| parent instanceof GreaterThanEquals || parent instanceof IsNullExpression
				|| parent instanceof LikeExpression)) {
			if (tm.containsKey(column.getWholeColumnName())) {// check for
																// duplicates in
																// tree map
				List<Expression> expressionList = tm.get(column.getWholeColumnName());
				for (int k = 0; k < expressionList.size(); k++) {
					if (parent != null) {
						if (expressionList.get(k).toString().equals(parent.toString())) {
							checked = true;
						}
					}
				}
				if (!checked)
					expressionList.add(parent);
				checked = false;
			} else {// save expression in list for corresponding column and add
					// to treemap
				List<Expression> expressionList = new ArrayList<Expression>();
				expressionList.add(parent);
				tm.put(column.getWholeColumnName(), expressionList);
			}
			stack.push(null);
			if (!(parent instanceof IsNullExpression))
				stack.push(null);
			expression = null;
		} else if (run.equals(2)) {// in run 2 we check for duplicates in larger
									// clauses and for conflicts with the
									// already found list of consolidated
									// predicates
			// we also store all predicates of this current clause
			if (tm.containsKey(column.getWholeColumnName())) {
				List<Expression> expressionListTemp = new ArrayList<Expression>(tm.get(column.getWholeColumnName()));
				for (int k = 0; k < expressionListTemp.size(); k++) {
					if (parent != null) {
						if (expressionListTemp.get(k).toString().equals(parent.toString())) {
							parent = null;
							right = null;
							expression = null;
							checked = true;
						}
					}
				}
				if (!checked) {// if no duplicate, check via consolidation if a
								// conflict is caused
					expressionListTemp.add(parent);
					TreeMap<String, List<Expression>> tmTemp = new TreeMap<String, List<Expression>>();
					tmTemp.clear();
					tmTemp.put(column.getWholeColumnName(), expressionListTemp);
					tmTemp = consolidateTreeMap(tmTemp);
					if (tmTemp.containsKey("FALSE")) {// conflict detected?
						parent = null;
						right = null;
						expression = null;
					} else if (parent instanceof IsNullExpression) {
						if (((IsNullExpression) parent).isNot()) {
							List<Expression> tempExpressionListOR = new ArrayList<Expression>();
							tempExpressionListOR.add(parent);
							tmOR.put(column.getWholeColumnName(), tempExpressionListOR);
						}
					} else if (parent instanceof MinorThan || parent instanceof MinorThanEquals
							|| parent instanceof GreaterThan || parent instanceof GreaterThanEquals) {
						if (tmOR.containsKey(column.getWholeColumnName())) {// check
																			// if
																			// larger
																			// or
																			// smaller
																			// right
																			// hand
																			// side
							List<Expression> tempExpressionListOR = new ArrayList<Expression>();
							tempExpressionListOR = tmOR.get(column.getWholeColumnName());
							tempExpressionListOR.add(parent);
						} else {
							List<Expression> expressionList = new ArrayList<Expression>();
							expressionList.add(parent);
							tmOR.put(column.getWholeColumnName(), expressionList);
						}
					}
				}
			} // since the column is not yet in the treemap add it to our tree
				// map of the current clause, this is the consolidated tree map
			else if (parent instanceof IsNullExpression) {
				if (((IsNullExpression) parent).isNot()) {
					List<Expression> tempExpressionListOR = new ArrayList<Expression>();
					tempExpressionListOR.add(parent);
					tmOR.put(column.getWholeColumnName(), tempExpressionListOR);
				}
			} else if (!(right instanceof BinaryExpression) && parent instanceof MinorThan
					|| parent instanceof MinorThanEquals || parent instanceof GreaterThan
					|| parent instanceof GreaterThanEquals) {
				if (tmOR.containsKey(column.getWholeColumnName())) {
					List<Expression> tempExpressionListOR = new ArrayList<Expression>();
					tempExpressionListOR = tmOR.get(column.getWholeColumnName());
					tempExpressionListOR.add(parent);
				} else {
					List<Expression> expressionList = new ArrayList<Expression>();
					expressionList.add(parent);
					tmOR.put(column.getWholeColumnName(), expressionList);
				}
			}
			stack.push(parent);
			if (!(parent instanceof IsNullExpression))
				stack.push(right);
		} else if (run.equals(3)) {// here we consolidate inside the larger
									// clauses using consolidateOR, we have
									// before collected all non-duplicate
									// predicates in the current clause and
									// consolidate them now
			if (parent instanceof MinorThan || parent instanceof MinorThanEquals || parent instanceof GreaterThan
					|| parent instanceof GreaterThanEquals) {
				parent = consolidateOR(column, parent);
				if (parent == null) {
					right = null;
					expression = null;
				}
			}
			if (parent != null) {
				if (stringListOR.contains(parent.toString())) {
					parent = null;
					right = null;
					expression = null;
				} else
					stringListOR.add(parent.toString());
			}
			stack.push(parent);
			if (!(parent instanceof IsNullExpression))
				stack.push(right);
		}
		stack.push(expression);
	}

	// visits the right hand side and left hand side of the and and ORs and only
	// if the left hand side is no binary expression itself
	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		Expression tempLeftExpression = binaryExpression.getLeftExpression();
		Expression tempRightExpression = binaryExpression.getRightExpression();
		if (!(binaryExpression instanceof OrExpression || binaryExpression instanceof AndExpression)
				&& (tempLeftExpression instanceof BinaryExpression)) {
			stack.push(tempRightExpression);
			stack.push(tempLeftExpression);
		} else {
			if (!(tempRightExpression instanceof Column) && !(tempRightExpression instanceof BinaryExpression
					&& !(tempRightExpression instanceof OrExpression) && !(tempRightExpression instanceof AndExpression)
					&& !(binaryExpression instanceof OrExpression || binaryExpression instanceof AndExpression)))
				tempRightExpression.accept(this);
			else
				stack.push(tempRightExpression);
			tempLeftExpression.accept(this);
		}
		Expression left = stack.pop();
		Expression right = stack.pop();
		Expression parent = stack.pop();
		if (parent != null) {// handle if any expression had to be deleted
			if (left == null)
				parent = right;
			else if (right == null)
				parent = left;
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

	// this function is for comparing the right hand side of all predicates. the
	// Parameter type is the type of the comparison: <, <= etc.
	// we compare here long, double and hexvalues with each other. due to the
	// structure of the hierarchy and the different types it needs to be this
	// complicated
	public Boolean compareNumbers(Expression expression1, Expression expression2, String type) {
		Boolean comparation = false;
		Boolean compared = false;
		Boolean tempExpression1Inverse = false;
		Boolean tempExpression2Inverse = false;
		Boolean equal = false;
		// We need to handle parenthesis and negative numbers such that the end
		// result is correct.
		Expression tempExpression1 = ((BinaryExpression) expression1).getRightExpression();
		if (tempExpression1 instanceof Parenthesis)
			tempExpression1 = ((Parenthesis) tempExpression1).getExpression();
		if (tempExpression1 instanceof InverseExpression) {
			tempExpression1 = ((InverseExpression) tempExpression1).getExpression();
			tempExpression1Inverse = true;
		}
		Expression tempExpression2 = ((BinaryExpression) expression2).getRightExpression();
		if (tempExpression2 instanceof Parenthesis)
			tempExpression2 = ((Parenthesis) tempExpression2).getExpression();
		if (tempExpression2 instanceof InverseExpression) {
			tempExpression2 = ((InverseExpression) tempExpression2).getExpression();
			tempExpression2Inverse = true;
		}
		// for each type of comparison we check the two right hand sides of the
		// predicates for their types and do the expected typecasting
		// Then check if one is larger or smaller etc.
		if (type.equals(">")) {
			if (tempExpression1 instanceof DoubleValue) {
				if (tempExpression2 instanceof DoubleValue) {
					compared = true;
					if (((DoubleValue) tempExpression1).getValue() > ((DoubleValue) tempExpression2).getValue())
						comparation = true;
					if (((Double) ((DoubleValue) tempExpression1).getValue())
							.equals(((DoubleValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof LongValue) {
					compared = true;
					if (((DoubleValue) tempExpression1).getValue() > ((LongValue) tempExpression2).getValue())
						comparation = true;
					if (((Long) ((LongValue) tempExpression2).getValue())
							.equals(((DoubleValue) tempExpression1).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof HexValue) {
					compared = true;
					if (((DoubleValue) tempExpression1).getValue() > Long
							.parseLong(((HexValue) tempExpression2).getValue()))
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression2).getValue())))
							.equals(((DoubleValue) tempExpression1).getValue()))
						equal = true;
				}
			}
			if (tempExpression1 instanceof LongValue) {
				if (tempExpression2 instanceof DoubleValue) {
					compared = true;
					if (((LongValue) tempExpression1).getValue() > ((DoubleValue) tempExpression2).getValue())
						comparation = true;
					if (((Long) ((LongValue) tempExpression1).getValue())
							.equals(((DoubleValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof LongValue) {
					compared = true;
					if (((LongValue) tempExpression1).getValue() > ((LongValue) tempExpression2).getValue())
						comparation = true;
					if (((Long) ((LongValue) tempExpression2).getValue())
							.equals(((LongValue) tempExpression1).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof HexValue) {
					compared = true;
					if (((LongValue) tempExpression1).getValue() > Long
							.parseLong(((HexValue) tempExpression2).getValue()))
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression2).getValue())))
							.equals(((LongValue) tempExpression1).getValue()))
						equal = true;
				}
			}
			if (tempExpression1 instanceof HexValue) {
				if (tempExpression2 instanceof DoubleValue) {
					compared = true;
					if (Long.parseLong(((HexValue) tempExpression1).getValue()) > ((DoubleValue) tempExpression2)
							.getValue())
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression1).getValue())))
							.equals(((DoubleValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof LongValue) {
					compared = true;
					if (Long.parseLong(((HexValue) tempExpression1).getValue()) > ((LongValue) tempExpression2)
							.getValue())
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression1).getValue())))
							.equals(((LongValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof HexValue) {
					compared = true;
					if (Long.parseLong(((HexValue) tempExpression1).getValue()) > Long
							.parseLong(((HexValue) tempExpression2).getValue()))
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression1).getValue())))
							.equals(Long.parseLong(((HexValue) tempExpression2).getValue())))
						equal = true;
				}
			}
		} else if (type.equals(">=")) {
			if (tempExpression1 instanceof DoubleValue) {
				if (tempExpression2 instanceof DoubleValue) {
					compared = true;
					if (((DoubleValue) tempExpression1).getValue() >= ((DoubleValue) tempExpression2).getValue())
						comparation = true;
					if (((Double) ((DoubleValue) tempExpression1).getValue())
							.equals(((DoubleValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof LongValue) {
					compared = true;
					if (((DoubleValue) tempExpression1).getValue() >= ((LongValue) tempExpression2).getValue())
						comparation = true;
					if (((Long) ((LongValue) tempExpression2).getValue())
							.equals(((DoubleValue) tempExpression1).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof HexValue) {
					compared = true;
					if (((DoubleValue) tempExpression1).getValue() >= Long
							.parseLong(((HexValue) tempExpression2).getValue()))
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression2).getValue())))
							.equals(((DoubleValue) tempExpression1).getValue()))
						equal = true;
				}
			}
			if (tempExpression1 instanceof LongValue) {
				if (tempExpression2 instanceof DoubleValue) {
					compared = true;
					if (((LongValue) tempExpression1).getValue() >= ((DoubleValue) tempExpression2).getValue())
						comparation = true;
					if (((Long) ((LongValue) tempExpression1).getValue())
							.equals(((DoubleValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof LongValue) {
					compared = true;
					if (((LongValue) tempExpression1).getValue() >= ((LongValue) tempExpression2).getValue())
						comparation = true;
					if (((Long) ((LongValue) tempExpression2).getValue())
							.equals(((LongValue) tempExpression1).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof HexValue) {
					compared = true;
					if (((LongValue) tempExpression1).getValue() >= Long
							.parseLong(((HexValue) tempExpression2).getValue()))
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression2).getValue())))
							.equals(((LongValue) tempExpression1).getValue()))
						equal = true;
				}
			}
			if (tempExpression1 instanceof HexValue) {
				if (tempExpression2 instanceof DoubleValue) {
					compared = true;
					if (Long.parseLong(((HexValue) tempExpression1).getValue()) >= ((DoubleValue) tempExpression2)
							.getValue())
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression1).getValue())))
							.equals(((DoubleValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof LongValue) {
					compared = true;
					if (Long.parseLong(((HexValue) tempExpression1).getValue()) >= ((LongValue) tempExpression2)
							.getValue())
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression1).getValue())))
							.equals(((LongValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof HexValue) {
					compared = true;
					if (Long.parseLong(((HexValue) tempExpression1).getValue()) >= Long
							.parseLong(((HexValue) tempExpression2).getValue()))
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression1).getValue())))
							.equals(Long.parseLong(((HexValue) tempExpression2).getValue())))
						equal = true;
				}
			}
		} else if (type.equals("<=")) {
			if (tempExpression1 instanceof DoubleValue) {
				if (tempExpression2 instanceof DoubleValue) {
					compared = true;
					if (((DoubleValue) tempExpression1).getValue() <= ((DoubleValue) tempExpression2).getValue())
						comparation = true;
					if (((Double) ((DoubleValue) tempExpression1).getValue())
							.equals(((DoubleValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof LongValue) {
					compared = true;
					if (((DoubleValue) tempExpression1).getValue() <= ((LongValue) tempExpression2).getValue())
						comparation = true;
					if (((Long) ((LongValue) tempExpression2).getValue())
							.equals(((DoubleValue) tempExpression1).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof HexValue) {
					compared = true;
					if (((DoubleValue) tempExpression1).getValue() <= Long
							.parseLong(((HexValue) tempExpression2).getValue()))
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression2).getValue())))
							.equals(((DoubleValue) tempExpression1).getValue()))
						equal = true;
				}
			}
			if (tempExpression1 instanceof LongValue) {
				if (tempExpression2 instanceof DoubleValue) {
					compared = true;
					if (((LongValue) tempExpression1).getValue() <= ((DoubleValue) tempExpression2).getValue())
						comparation = true;
					if (((Long) ((LongValue) tempExpression1).getValue())
							.equals(((DoubleValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof LongValue) {
					compared = true;
					if (((LongValue) tempExpression1).getValue() <= ((LongValue) tempExpression2).getValue())
						comparation = true;
					if (((Long) ((LongValue) tempExpression2).getValue())
							.equals(((LongValue) tempExpression1).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof HexValue) {
					compared = true;
					if (((LongValue) tempExpression1).getValue() <= Long
							.parseLong(((HexValue) tempExpression2).getValue()))
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression2).getValue())))
							.equals(((LongValue) tempExpression1).getValue()))
						equal = true;
				}
			}
			if (tempExpression1 instanceof HexValue) {
				if (tempExpression2 instanceof DoubleValue) {
					compared = true;
					if (Long.parseLong(((HexValue) tempExpression1).getValue()) <= ((DoubleValue) tempExpression2)
							.getValue())
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression1).getValue())))
							.equals(((DoubleValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof LongValue) {
					compared = true;
					if (Long.parseLong(((HexValue) tempExpression1).getValue()) <= ((LongValue) tempExpression2)
							.getValue())
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression1).getValue())))
							.equals(((LongValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof HexValue) {
					compared = true;
					if (Long.parseLong(((HexValue) tempExpression1).getValue()) <= Long
							.parseLong(((HexValue) tempExpression2).getValue()))
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression1).getValue())))
							.equals(Long.parseLong(((HexValue) tempExpression2).getValue())))
						equal = true;
				}
			}
		} else if (type.equals("<")) {
			if (tempExpression1 instanceof DoubleValue) {
				if (tempExpression2 instanceof DoubleValue) {
					compared = true;
					if (((DoubleValue) tempExpression1).getValue() < ((DoubleValue) tempExpression2).getValue())
						comparation = true;
					if (((Double) ((DoubleValue) tempExpression1).getValue())
							.equals(((DoubleValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof LongValue) {
					compared = true;
					if (((DoubleValue) tempExpression1).getValue() < ((LongValue) tempExpression2).getValue())
						comparation = true;
					if (((Long) ((LongValue) tempExpression2).getValue())
							.equals(((DoubleValue) tempExpression1).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof HexValue) {
					compared = true;
					if (((DoubleValue) tempExpression1).getValue() < Long
							.parseLong(((HexValue) tempExpression2).getValue()))
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression2).getValue())))
							.equals(((DoubleValue) tempExpression1).getValue()))
						equal = true;
				}
			}
			if (tempExpression1 instanceof LongValue) {
				if (tempExpression2 instanceof DoubleValue) {
					compared = true;
					if (((LongValue) tempExpression1).getValue() < ((DoubleValue) tempExpression2).getValue())
						comparation = true;
					if (((Long) ((LongValue) tempExpression1).getValue())
							.equals(((DoubleValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof LongValue) {
					compared = true;
					if (((LongValue) tempExpression1).getValue() < ((LongValue) tempExpression2).getValue())
						comparation = true;
					if (((Long) ((LongValue) tempExpression2).getValue())
							.equals(((LongValue) tempExpression1).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof HexValue) {
					compared = true;
					if (((LongValue) tempExpression1).getValue() < Long
							.parseLong(((HexValue) tempExpression2).getValue()))
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression2).getValue())))
							.equals(((LongValue) tempExpression1).getValue()))
						equal = true;
				}
			}
			if (tempExpression1 instanceof HexValue) {
				if (tempExpression2 instanceof DoubleValue) {
					compared = true;
					if (Long.parseLong(((HexValue) tempExpression1).getValue()) < ((DoubleValue) tempExpression2)
							.getValue())
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression1).getValue())))
							.equals(((DoubleValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof LongValue) {
					compared = true;
					if (Long.parseLong(((HexValue) tempExpression1).getValue()) < ((LongValue) tempExpression2)
							.getValue())
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression1).getValue())))
							.equals(((LongValue) tempExpression2).getValue()))
						equal = true;
				} else if (tempExpression2 instanceof HexValue) {
					compared = true;
					if (Long.parseLong(((HexValue) tempExpression1).getValue()) < Long
							.parseLong(((HexValue) tempExpression2).getValue()))
						comparation = true;
					if (((Long) (Long.parseLong(((HexValue) tempExpression1).getValue())))
							.equals(Long.parseLong(((HexValue) tempExpression2).getValue())))
						equal = true;
				}
			}
		}
		// if they could not be compared for any reason return null (right hand
		// side is no number for example)
		if (!compared)
			return null;
		Integer inverse = tempExpression1Inverse.compareTo(tempExpression2Inverse);
		if (inverse.equals(0)) {// have to check if one or both where negative
								// numbers
			if (tempExpression1Inverse && !equal) {
				return !comparation;
			} else
				return comparation;
		} else if ((inverse < 0 && type.equals("<")) || (inverse < 0 && type.equals(">="))
				|| (inverse > 0 && type.equals(">")) || (inverse > 0 && type.equals("<="))) {
			return !comparation;
		} else
			return comparation;
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
		if (run > 1) {// in run one we do not care about clauses with more than
						// one literal
			tmOR.clear();
			stringListOR.clear();
			Expression tempExpression = parenthesis.getExpression();
			tempExpression.accept(this);
			tempExpression = stack.pop();
			if (!tmOR.isEmpty()) {// if current treemap is not empty it means we
									// have more than one literal with the same
									// column. we need to consolidate it and do
									// a run 3!
				run = 3;
				// System.out.println("tmOR: "+tmOR);
				tempExpression.accept(this);
				tempExpression = stack.pop();
				run = 2;
			}
			parenthesis = (Parenthesis) stack.pop();
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
		stack.push(addition);
		visitBinaryExpression(addition);
	}

	@Override
	public void visit(Division division) {
		stack.push(division);
		visitBinaryExpression(division);
	}

	@Override
	public void visit(Multiplication multiplication) {
		stack.push(multiplication);
		visitBinaryExpression(multiplication);
	}

	@Override
	public void visit(Subtraction subtraction) {
		stack.push(subtraction);
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
		stack.push(equalsTo);
		visitBinaryExpression(equalsTo);
	}

	@Override
	public void visit(GreaterThan greaterThan) {
		stack.push(greaterThan);
		visitBinaryExpression(greaterThan);
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		stack.push(greaterThanEquals);
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
		stack.push(likeExpression);
		visitBinaryExpression(likeExpression);
	}

	@Override
	public void visit(MinorThan minorThan) {
		stack.push(minorThan);
		visitBinaryExpression(minorThan);
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		stack.push(minorThanEquals);
		visitBinaryExpression(minorThanEquals);
	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		stack.push(notEqualsTo);
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
		stack.push(concat);
		visitBinaryExpression(concat);
	}

	@Override
	public void visit(Matches matches) {
	}

	@Override
	public void visit(BitwiseAnd bitwiseAnd) {
		stack.push(bitwiseAnd);
		visitBinaryExpression(bitwiseAnd);
	}

	@Override
	public void visit(BitwiseOr bitwiseOr) {
		stack.push(bitwiseOr);
		visitBinaryExpression(bitwiseOr);
	}

	@Override
	public void visit(BitwiseXor bitwiseXor) {
		stack.push(bitwiseXor);
		visitBinaryExpression(bitwiseXor);
	}

	@Override
	public void visit(CastExpression cast) {
	}

	@Override
	public void visit(Modulo modulo) {
		stack.push(modulo);
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