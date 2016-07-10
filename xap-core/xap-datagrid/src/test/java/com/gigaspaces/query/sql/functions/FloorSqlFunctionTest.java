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
public class FloorSqlFunctionTest {

    private FloorSqlFunction floorSqlFunction;

    @Before
    public void setUp() throws Exception {
        floorSqlFunction = new FloorSqlFunction();
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
                return -1;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(-1));
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
                return -1.22;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(-2.0));
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
                return (float) -1.22;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(-2f));
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
                return (long) -3;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(-3L));
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
                return (short) -3;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals((short) -3));
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
                return (byte) -1;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals((byte) -1));
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
                return 1;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(1));
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
                return 0.22;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(0.0));
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
                return 0.22f;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(0f));
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
                return 3L;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(3L));
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
                return (short) 3;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals((short) 3));
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
                return (byte) 1;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals((byte) 1));
    }

    @Test
    public void testApplyZeroInteger() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 0;
            }

        };

        Object res = floorSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(0));
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
                return "a";
            }

        };

        floorSqlFunction.apply(sqlFunctionExecutionContext);
    }
}