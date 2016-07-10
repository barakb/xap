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
 * Created by yaeln on 3/16/16.
 */
@com.gigaspaces.api.InternalApi
public class AbsSqlFunctionApplyTest {

    private AbsSqlFunction absSqlFunction;

    @Before
    public void setUp() throws Exception {
        absSqlFunction = new AbsSqlFunction();
    }

    @Test
    public void testApplyPositiveInteger() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 5;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(5));
    }

    @Test
    public void testApplyPositiveDouble() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 5d;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(5.0));
    }

    @Test
    public void testApplyPositiveLong() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 5L;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(5L));
    }

    @Test
    public void testApplyPositiveFloat() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 5f;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(5f));
    }

    @Test
    public void testApplyPositiveShort() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return (short) 5;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals((short) 5));

    }

    @Test
    public void testApplyPositiveByte() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return (byte) 5;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals((byte) 5));
    }

    @Test
    public void testApplyNegativeInteger() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return -5;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(5));
    }

    @Test
    public void testApplyNegativeDouble() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return (double) -5;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(5.0));
    }

    @Test
    public void testApplyNegativeLong() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return (long) -5;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(5L));
    }

    @Test
    public void testApplyNegativeFloat() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return (float) -5;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(5f));
    }

    @Test
    public void testApplyNegativeShort() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return (short) -5;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals((short) 5));
    }

    @Test
    public void testApplyNegativeByte() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return (byte) -5;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals((byte) 5));
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
                return "A STRING";
            }
        };

        absSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test
    public void testApplyZero() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 0d;
            }
        };

        Object res = absSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(0.0));
    }


}