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

package com.gigaspaces.persistency.qa.utest.parser;

import com.gigaspaces.persistency.parser.SQL2MongoBaseVisitor;
import com.gigaspaces.persistency.parser.SQL2MongoLexer;
import com.gigaspaces.persistency.parser.SQL2MongoParser;
import com.gigaspaces.persistency.parser.SQL2MongoParser.ParseContext;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class SQL2MongoBaseVisitorTest {

    private static final String LIKE = "like()";
    private static final String PARAMETER_PLACEHOLDER = "'%{}'";

    @Test
    public void testEquals() {
        DBObject actual = parse("number = ?").getQuery();

        DBObject expected = QueryBuilder.start("number")
                .is(PARAMETER_PLACEHOLDER).get();

        assertEquals(expected, actual);

    }

    @Test
    public void testNotEquals() {
        DBObject actual = parse("number != ?").getQuery();

        DBObject expected = QueryBuilder.start("number")
                .notEquals(PARAMETER_PLACEHOLDER).get();

        assertEquals(expected, actual);
    }

    @Test
    public void testGreaterThan() {

        DBObject actual = parse("number > ?").getQuery();

        DBObject expected = QueryBuilder.start("number")
                .greaterThan(PARAMETER_PLACEHOLDER).get();

        assertEquals(expected, actual);

    }

    public void testGreaterThanEquals() {
        DBObject actual = parse("number >= ?").getQuery();

        DBObject expected = QueryBuilder.start("number")
                .notEquals(PARAMETER_PLACEHOLDER).get();

        assertEquals(expected, actual);

    }

    @Test
    public void testLessThan() {
        DBObject actual = parse("number < ?").getQuery();

        DBObject expected = QueryBuilder.start("number")
                .lessThan(PARAMETER_PLACEHOLDER).get();

        assertEquals(expected, actual);

    }

    @Test
    public void testLessThanEquals() {
        DBObject actual = parse("number <= ?").getQuery();

        DBObject expected = QueryBuilder.start("number")
                .lessThanEquals(PARAMETER_PLACEHOLDER).get();

        assertEquals(expected, actual);
    }

    @Test
    public void testRlike() {

        DBObject actual = parse("number rlike ?").getQuery();

        DBObject expected = QueryBuilder.start("number")
                .regex(Pattern.compile("rlike()")).get();

        assertEquals(expected, actual);
    }

    @Test
    public void testLike() {
        DBObject actual = parse("number like ?").getQuery();

        DBObject expected = QueryBuilder.start("number")
                .regex(Pattern.compile(LIKE)).get();

        assertEquals(expected, actual);
    }

    @Test
    public void testSimpleAnd() {
        DBObject actual = parse("number is NOT null AND name like ?")
                .getQuery();

        DBObject expected = QueryBuilder.start().and("number").exists(false)
                .and("name").regex(Pattern.compile(LIKE)).get();

        assertEquals(expected, actual);
    }

    @Test
    public void testMultipleAnd() {
        DBObject actual = parse(
                "number is NOT null AND name like ? AND age >= ?").getQuery();

        DBObject expexted = QueryBuilder.start().and("number").exists(false)
                .and("name").regex(Pattern.compile(LIKE)).and("age")
                .greaterThanEquals(PARAMETER_PLACEHOLDER).get();

        assertEquals(expexted, actual);
    }

    @Test
    public void testSimpleOr() {
        DBObject actual = parse("x < ? OR y <= ?").getQuery();

        DBObject expected = QueryBuilder
                .start()
                .or(QueryBuilder.start("x").lessThan(PARAMETER_PLACEHOLDER)
                        .get())
                .or(QueryBuilder.start("y")
                        .lessThanEquals(PARAMETER_PLACEHOLDER).get()).get();

        assertEquals(expected, actual);
    }

    @Test
    public void testMultipleOr() {
        SQL2MongoBaseVisitor<ParseContext> visitor = parse("x < ? OR y <= ? OR z = ?");

        QueryBuilder qb = QueryBuilder
                .start()
                .or(QueryBuilder.start("x").lessThan(PARAMETER_PLACEHOLDER)
                        .get())
                .or(QueryBuilder.start("y")
                        .lessThanEquals(PARAMETER_PLACEHOLDER).get())
                .or(QueryBuilder.start("z").is(PARAMETER_PLACEHOLDER).get());

        assertEquals(qb.get(), visitor.getQuery());
    }

    @Test
    public void testAndOr() {
        DBObject actual = parse("x < ? AND y <= ? OR z = ?").getQuery();

        DBObject expected = QueryBuilder
                .start()
                .or(QueryBuilder.start().and("x")
                                .lessThan(PARAMETER_PLACEHOLDER).and("y")
                                .lessThanEquals(PARAMETER_PLACEHOLDER).get(),
                        QueryBuilder.start("z").is(PARAMETER_PLACEHOLDER).get())
                .get();

        assertEquals(expected, actual);
    }

    @Test
    public void testOrWithTwoAndClause() {
        DBObject actual = parse("x < ? AND y <= ? OR z = ? AND w like ?")
                .getQuery();

        DBObject expcted = QueryBuilder
                .start()
                .or(QueryBuilder.start().and("x")
                                .lessThan(PARAMETER_PLACEHOLDER).and("y")
                                .lessThanEquals(PARAMETER_PLACEHOLDER).get(),
                        QueryBuilder.start("z").is(PARAMETER_PLACEHOLDER)
                                .and("w").regex(Pattern.compile(LIKE)).get())
                .get();

        assertEquals(expcted, actual);
    }

    @Test
    public void testAndWithTwoOrClause() {
        DBObject actual = parse("x < ? OR y <= ? AND z = ? OR w like ?")
                .getQuery();

        DBObject expected = QueryBuilder
                .start()
                .or(QueryBuilder.start().and("x")
                                .lessThan(PARAMETER_PLACEHOLDER).and("y")
                                .lessThanEquals(PARAMETER_PLACEHOLDER).and("z")
                                .is(PARAMETER_PLACEHOLDER).get(),
                        QueryBuilder.start("w").regex(Pattern.compile(LIKE))
                                .get()).get();

        assertEquals(expected, actual);
    }

    /**
     * @return
     */
    private SQL2MongoBaseVisitor<ParseContext> parse(String line) {
        ANTLRInputStream charstream = new ANTLRInputStream(line);

        SQL2MongoLexer lexer = new SQL2MongoLexer(charstream);

        TokenStream tokenStream = new CommonTokenStream(lexer);

        SQL2MongoParser parser = new SQL2MongoParser(tokenStream);

        SQL2MongoBaseVisitor<ParseContext> visitor = new SQL2MongoBaseVisitor<ParseContext>();

        parser.parse().accept(visitor);

        return visitor;
    }

}