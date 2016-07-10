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
 * Created by yaeln on 3/20/16.
 */
@com.gigaspaces.api.InternalApi
public class UpperSqlFunctionTest {

    private UpperSqlFunction upperSqlFunction;

    @Before
    public void setUp() throws Exception {
        upperSqlFunction = new UpperSqlFunction();
    }

    @Test(expected = RuntimeException.class)
    public void testApplyNullString() throws Exception {
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

        upperSqlFunction.apply(sqlFunctionExecutionContext);

    }

    @Test
    public void testApplyLowerCaseString() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return "abc";
            }
        };

        Object res = upperSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals("ABC"));

    }

    @Test
    public void testApplyUpperCaseString() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return "ABC";
            }
        };

        Object res = upperSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals("ABC"));

    }

    @Test
    public void testApplyLowerAndUpperCaseString() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return "AbC";
            }
        };

        Object res = upperSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals("ABC"));

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

        Object res = upperSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(""));
    }

    @Test
    public void testApplySpaceDeliminatorString() throws Exception {
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

        Object res = upperSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(" "));

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

        upperSqlFunction.apply(sqlFunctionExecutionContext);

    }

}