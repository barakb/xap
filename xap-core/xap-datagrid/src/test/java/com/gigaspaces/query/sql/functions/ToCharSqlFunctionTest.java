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

import java.util.Date;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by yaeln on 3/17/16.
 */
@com.gigaspaces.api.InternalApi
public class ToCharSqlFunctionTest {

    private ToCharSqlFunction toCharSqlFunction;

    @Before
    public void setUp() throws Exception {
        toCharSqlFunction = new ToCharSqlFunction();
    }

    @Test
    public void testApplyNumberWithDecimalPointFormat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 1.234;
                return "####.0";
            }
        };

        Object res = toCharSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        String s = "1.2";
        assertTrue(s.equals(res));
    }

    @Test
    public void testApplyNumberWithCommaFormat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 1000.25;
                return "0,000.00";
            }
        };

        Object res = toCharSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        String s = "1,000.25";
        assertTrue(s.equals(res));
    }

    @Test
    public void testApplyNumberWithDollarFormat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 1000.25;
                return "$0,000.00";
            }
        };

        Object res = toCharSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        String s = "$1,000.25";
        assertTrue(s.equals(res));
    }

    @Test
    public void testApplyNumberWithLeadingZerosFormat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 25;
                return "'0000'00";
            }
        };

        Object res = toCharSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        String s = "000025";
        assertTrue(s.equals(res));
    }

    @Test
    public void testApplyDateTimeHyphenFormat() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return new Date(1458469071702L);
                return "dd-M-yyyy hh:mm:ss";
            }
        };

        Object res = toCharSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        String s = "20-3-2016 10:17:51";
        assertTrue(s.equals(res));
    }
}