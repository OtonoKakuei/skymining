//New File by FlorianB for SQLQueryLogTransformer: New Expression Type

/*
 * #%L
 * JSQLParser library
 * %%
 * Copyright (C) 2004 - 2013 JSQLParser
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as 
 * published by the Free Software Foundation, either version 2.1 of the 
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public 
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-2.1.html>.
 * #L%
 */
package net.sf.jsqlparser.expression;

/**
 * Every number with a point or a exponential format is a DoubleValue
 */
public class BooleanValue implements Expression {

	private boolean value;

	public BooleanValue(final boolean value) {
		this.value = value;
	}

	@Override
	public void accept(ExpressionVisitor expressionVisitor) {
		expressionVisitor.visit(this);
	}

	public boolean getValue() {
		return value;
	}

	public void setValue(boolean b) {
		value = b;
	}

	@Override
	public String toString() {
		return new Boolean(value).toString().toUpperCase();
	}
}
