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
public class AppendSqlFunctionTest {

    private AppendSqlFunction appendSqlFunction;

    @Before
    public void setUp() throws Exception {
        appendSqlFunction = new AppendSqlFunction();
    }

    @Test
    public void testApplyNullValue() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return "a";
                }
                return null;
            }
        };

        Object res = appendSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        String string = "anull";
        assertTrue(string.equals(res));
    }

    @Test
    public void testApplyTwoEmptyStrings() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                return "";
            }
        };

        Object res = appendSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue("".equals(res));
    }

    @Test
    public void testApplyNotEmptyStringWithEmptyString() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return "a";
                }
                return "";
            }
        };

        Object res = appendSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue("a".equals(res));
    }

    @Test
    public void testApplyTwoStrings() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return "a";
                }
                return "b";
            }
        };

        Object res = appendSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue("ab".equals(res));
    }

}