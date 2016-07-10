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
public class InStrSqlFunctionTest {

    private InStrSqlFunction inStrSqlFunction;

    @Before
    public void setUp() throws Exception {
        inStrSqlFunction = new InStrSqlFunction();
    }

    @Test
    public void applyTwoEmptyStrings() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return "";
                } else if (index == 1) {
                    return "";
                } else throw new RuntimeException("bad argument number");
            }
        };

        Object apply = inStrSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(apply);
        assertTrue(apply.equals(1));
    }

    @Test(expected = RuntimeException.class)
    public void applyNullLeft() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return null;
                } else if (index == 1) {
                    return "str";
                } else throw new RuntimeException("bad argument number");
            }
        };
        inStrSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test(expected = RuntimeException.class)
    public void applyNullRight() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return "str";
                } else if (index == 1) {
                    return null;
                } else throw new RuntimeException("bad argument number");
            }
        };
        inStrSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test
    public void applySimpleCase() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return "string";
                } else if (index == 1) {
                    return "ring";
                } else throw new RuntimeException("bad argument number");
            }
        };

        Object apply = inStrSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(apply);
        assertTrue(apply.equals(3));
    }

    @Test
    public void applyNotSubString() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return "string";
                } else if (index == 1) {
                    return "not";
                } else throw new RuntimeException("bad argument number");
            }
        };

        Object apply = inStrSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(apply);
        assertTrue(apply.equals(0));
    }

    @Test
    public void applySubStringAt0() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return "string";
                } else if (index == 1) {
                    return "str";
                } else throw new RuntimeException("bad argument number");
            }
        };

        Object apply = inStrSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(apply);
        assertTrue(apply.equals(1));
    }

    @Test
    public void applyMultiSubString() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return "stringString";
                } else if (index == 1) {
                    return "ring";
                } else throw new RuntimeException("bad argument number");
            }
        };

        Object apply = inStrSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(apply);
        assertTrue(apply.equals(3));
    }

    @Test(expected = RuntimeException.class)
    public void applyWrongTypeLeft() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return 1;
                } else if (index == 1) {
                    return "ring";
                } else throw new RuntimeException("bad argument number");
            }
        };

        inStrSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test(expected = RuntimeException.class)
    public void applyWrongTypeRight() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return "str";
                } else if (index == 1) {
                    return 42;
                } else throw new RuntimeException("bad argument number");
            }
        };

        inStrSqlFunction.apply(sqlFunctionExecutionContext);
    }
}