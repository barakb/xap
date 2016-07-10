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

package com.gigaspaces.query.sql.functions;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by yaeln on 3/17/16.
 */
@com.gigaspaces.api.InternalApi
public class CharLengthSqlFunctionTest {

    private CharLengthSqlFunction charLengthSqlFunction;

    @Before
    public void setUp() throws Exception {
        charLengthSqlFunction = new CharLengthSqlFunction();
    }

    @Test
    public void testApplyEmptyString() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return "";
            }
        };

        Object res = charLengthSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(0));
    }

    @Test(expected = RuntimeException.class)
    public void testApplyNullField() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return null;
            }
        };

        charLengthSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test
    public void testApplySpaceDelimiter() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return " ";
            }
        };

        Object res = charLengthSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(1));
    }

    @Test
    public void testApplyOneChar() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return "a";
            }
        };

        Object res = charLengthSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(1));
    }

    @Test
    public void testApplyRegularString() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return "abcd";
            }
        };

        Object res = charLengthSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(4));
    }

    @Test
    public void testApplyCharArray() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                char[] c = new char[3];
                c[0] = 'a';
                c[1] = 'b';
                c[2] = 'c';
                return c;
            }
        };

        Object res = charLengthSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(3));
    }

    @Test
    public void testApplySingleChar() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 'a';
            }
        };

        Object res = charLengthSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(1));
    }

    @Test(expected = RuntimeException.class)
    public void testApplyWrongType() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 1;
            }
        };

        charLengthSqlFunction.apply(sqlFunctionExecutionContext);
    }

}