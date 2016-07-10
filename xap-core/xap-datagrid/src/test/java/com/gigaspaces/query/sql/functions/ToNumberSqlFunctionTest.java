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
public class ToNumberSqlFunctionTest {

    private ToNumberSqlFunction toNumberSqlFunction;

    @Before
    public void setUp() throws Exception {
        toNumberSqlFunction = new ToNumberSqlFunction();
    }

    @Test
    public void applyOneArgDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return "1210.73";
            }
        };

        Object res = toNumberSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        Double d = 1210.73;
        assertTrue(d.equals(res));
    }

    @Test
    public void applyOneArgInteger() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return "567";
            }
        };

        Object res = toNumberSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        Integer i = 567;
        assertTrue(i.equals(res));
    }

    @Test
    public void applyOneArgDoubleWithFormat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return "1210.73";
                }
                return "###0.0";
            }
        };

        Object res = toNumberSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        Double d = 1210.7;
        assertTrue(d.equals(res));
    }

    @Test(expected = RuntimeException.class)
    public void applyOneArgIntegerWithComma() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return "1,000";
            }
        };

        Object res = toNumberSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        Integer i = 1000;
        assertTrue(i.equals(res));
    }

    @Test(expected = RuntimeException.class)
    public void applyWrongType() throws Exception {

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
        toNumberSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test(expected = RuntimeException.class)
    public void applyWrongType_1() throws Exception {

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

        toNumberSqlFunction.apply(sqlFunctionExecutionContext);
    }
}