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
public class ModSqlFunctionTest {

    private ModSqlFunction modSqlFunction;

    @Before
    public void setUp() throws Exception {
        modSqlFunction = new ModSqlFunction();
    }

    @Test
    public void applyDoubleModInt() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 34.5;
                return 3;
            }
        };

        Object res = modSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(1.5));
    }

    @Test
    public void applyIntModDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 45;
                return 3.5;
            }
        };

        Object res = modSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(3.0));
    }

    @Test
    public void applyIntModInt() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 234;
                return 10;
            }
        };

        Object res = modSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(4));
    }

    @Test
    public void applyDoubleModDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 62.3;
                return 3.5;
            }
        };

        Object res = modSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(2.799999999999997));
    }

    @Test
    public void applyDoubleModLong() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 62.3;
                return 7L;
            }
        };

        Object res = modSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(6.299999999999997));
    }

    @Test
    public void applyLongModDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 11L;
                return 9.2;
            }
        };

        Object res = modSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(1.8000000000000007));
    }

    @Test
    public void applyIntegerModLong() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 5;
                return 2L;
            }
        };

        Object res = modSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(1L));
    }

    @Test
    public void applyLongModInteger() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 5L;
                return 2;
            }
        };

        Object res = modSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(1L));
    }

    @Test
    public void applyLongModLong() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 5L;
                return 2L;
            }
        };

        Object res = modSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(1L));
    }

    @Test(expected = RuntimeException.class)
    public void applyNullModNull() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                return null;
            }
        };

        modSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test(expected = RuntimeException.class)
    public void applyNullModInt() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return null;
                return 3;
            }
        };

        modSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test(expected = RuntimeException.class)
    public void applyIntModNull() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 10;
                return null;
            }
        };

        modSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test(expected = RuntimeException.class)
    public void applyStringModInt() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return "str";
                return 3;
            }
        };

        modSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test(expected = RuntimeException.class)
    public void applyIntModString() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 10;
                return "str";
            }
        };

        modSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test
    public void applySmallModBig() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 3;
                return 7;
            }
        };

        Object res = modSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(3));
    }
}