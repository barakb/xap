/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gigaspaces.persistency.parser;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.LinkedList;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Pattern;

public class SQL2MongoBaseVisitor<T> extends AbstractParseTreeVisitor<T>
        implements SQL2MongoVisitor<T> {

    private static final String RLIKE = "rlike()";

    private static final String LIKE = "like()";

    private static final String PARAMETER_PLACEHOLDER = "'%{}'";

    private static final Pattern stringPattern = Pattern.compile("'[^']*'");
    private static final Pattern booleanPattern = Pattern.compile("(true|false)");
    private static final Pattern numberPattern = Pattern.compile("[0-9\\.]+");


    private DBObject query;

    private final Stack<String> stack = new Stack<String>();

    private QueryBuilder atom = QueryBuilder.start();
    private final LinkedList<DBObject> ands = new LinkedList<DBObject>();
    private final LinkedList<DBObject> ors = new LinkedList<DBObject>();

    public SQL2MongoBaseVisitor() {
        this.query = new BasicDBObject();
    }

    public T visitNot(SQL2MongoParser.NotContext ctx) {
        return visitChildren(ctx);
    }

    public DBObject getQuery() {
        return query;
    }

    public T visitExpression(SQL2MongoParser.ExpressionContext ctx) {
        return visitChildren(ctx);
    }

    public T visitAtom(SQL2MongoParser.AtomContext ctx) {

        if (ctx.getChildCount() > 0) {
            String id = ctx.getChild(0).getText();

            stack.push(id);
        }

        return visitChildren(ctx);
    }

    public T visitOp(SQL2MongoParser.OpContext ctx) {
        String op = ctx.getText();

        stack.push(op);

        return visitChildren(ctx);
    }

    public T visitOr(SQL2MongoParser.OrContext ctx) {

        T r = visitChildren(ctx);

        if (IsLogic(ctx, "OR")) {

            QueryBuilder q = QueryBuilder.start();

            for (DBObject at : ands)
                q.or(at);

            Set<String> keys = atom.get().keySet();

            for (String key : keys) {
                q.or(new BasicDBObject(key, atom.get().get(key)));
            }

            ands.clear();
            atom = QueryBuilder.start();
            ors.add(q.get());
        }

        return r;
    }

    public T visitValue(SQL2MongoParser.ValueContext ctx) {

        T r = visitChildren(ctx);

        String op = stack.pop();
        String id = stack.pop();

        String val = ctx.getChild(0).getText();

        atom.and(id);

        if ("is".equals(op)) {
            buildIsExpression(val, atom);
        } else if ("like".equals(op)) {
            atom.regex(Pattern.compile(LIKE));
        } else if ("rlike".equals(op)) {
            atom.regex(Pattern.compile(RLIKE));
        } else if ("!=".equals(op)) {
            atom.notEquals(PARAMETER_PLACEHOLDER /* evaluate(val) */);
        } else if ("=".equals(op)) {
            atom.is(evaluate(val));
        } else if (">=".equals(op)) {
            atom.greaterThanEquals(PARAMETER_PLACEHOLDER/* evaluate(val) */);
        } else if ("<=".equals(op)) {
            atom.lessThanEquals(PARAMETER_PLACEHOLDER /* evaluate(val) */);
        } else if ("<".equals(op)) {
            atom.lessThan(PARAMETER_PLACEHOLDER /* val */);
        } else if (">".equals(op)) {
            atom.greaterThan(PARAMETER_PLACEHOLDER /* val */);
        }

        return r;
    }

    /**
     * Evaluate string presentation to object
     *
     * @param val - string presentation of value
     * @return Object instance type
     */
    private Object evaluate(String val) {

        if (val == null || val.isEmpty())
            return null;

        if ("?".equals(val))
            return PARAMETER_PLACEHOLDER;

        // test if value is String
        boolean isValue = stringPattern.matcher(val).matches();
        if (isValue) {
            return val.substring(1, val.length() - 1);
        }

        // test if value is true | false
        isValue = booleanPattern.matcher(val).matches();
        if (isValue) {
            return Boolean.valueOf(val);
        }

        // test if number value
        isValue = numberPattern.matcher(val).matches();
        if (isValue) {
            int floatIndex = val.indexOf('.');

            if (floatIndex > -1) {
                return Double.valueOf(val);
            }

            return Long.valueOf(val);
        }

        throw new IllegalArgumentException(val);
    }

    public T visitParse(SQL2MongoParser.ParseContext ctx) {

        T r = visitChildren(ctx);

        for (DBObject o : ors) {
            query.putAll(o);
        }

        for (DBObject o : ands)
            query.putAll(o);

        DBObject o = atom.get();

        if (o.keySet().size() > 0)
            query.putAll(o);

        return r;
    }

    public T visitAnd(SQL2MongoParser.AndContext ctx) {

        T r = visitChildren(ctx);

        if (IsLogic(ctx, "AND")) {

            ands.add(atom.get());

            atom = QueryBuilder.start();
        }

        return r;
    }

    private boolean IsLogic(ParserRuleContext ctx, String text) {

        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree c = ctx.getChild(i);

            if (c.getText().equals(text))
                return true;
        }
        return false;
    }

    private void buildIsExpression(String val, QueryBuilder subQuery) {

        int index = val.indexOf("NOT");

        subQuery.exists(!(index > -1));
    }

}