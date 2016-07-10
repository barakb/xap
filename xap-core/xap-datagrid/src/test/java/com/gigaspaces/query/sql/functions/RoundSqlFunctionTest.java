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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by yaeln on 3/17/16.
 */
@com.gigaspaces.api.InternalApi
public class RoundSqlFunctionTest {

    private RoundSqlFunction roundSqlFunction;

    @Before
    public void setUp() throws Exception {
        roundSqlFunction = new RoundSqlFunction();
    }

    @Test
    public void testApplyOneArgZeroDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 0.0;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(0.0));
    }

    @Test
    public void testApplyOneArgPositiveRoundUpDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 1.55;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(2.0));
    }

    @Test
    public void testApplyOneArgPositiveRoundDownDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 1.45;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(1.0));
    }

    @Test
    public void testApplyOneArgNegativeRoundUpDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return -1.55;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(-2.0));
    }

    @Test
    public void testApplyOneArgNegativeRoundDownDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return -1.45;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(-1.0));
    }

    @Test
    public void testApplyTwoArgsTwoDecimalPointsDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726;
                return 2;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(873.730));
    }

    @Test
    public void testApplyTwoArgsOneDecimalPointDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726;
                return 1;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(873.700));
    }

    @Test
    public void testApplyTwoArgsZeroDecimalPointsDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726;
                return 0;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(874.000));
    }

    @Test
    public void testApplyTwoArgsMinusOneDecimalPointDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726;
                return -1;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(870.000));
    }

    @Test
    public void testApplyTwoArgsMinusTwoDecimalPointsDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726;
                return -2;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(900.000));
    }

    @Test
    public void testApplyTwoArgsMinusThreeDecimalPointsDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726;
                return -3;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(1000.000));
    }

    @Test
    public void testApplyTwoArgsMinusFourDecimalPointsDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726;
                return -4;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(0.000));
    }

    //#################################################################################//

    @Test
    public void testApplyOneArgZeroFloat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 0.0;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(0.0));
    }

    @Test
    public void testApplyOneArgPositiveRoundUpFloat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 1.55f;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(2.0f));
    }

    @Test
    public void testApplyOneArgPositiveRoundDownFloat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 1.45f;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(1.0f));
    }

    @Test
    public void testApplyOneArgNegativeRoundUpFloat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return -1.55f;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(-2.0f));
    }

    @Test
    public void testApplyOneArgNegativeRoundDownFloat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return -1.45f;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(-1.0f));
    }

    @Test
    public void testApplyTwoArgsTwoDecimalPointsFloat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726f;
                return 2;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(873.730f));
    }

    @Test
    public void testApplyTwoArgsOneDecimalPointFloat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726f;
                return 1;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(873.700f));
    }

    @Test
    public void testApplyTwoArgsZeroDecimalPointsFloat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726f;
                return 0;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(874.000f));
    }

    @Test
    public void testApplyTwoArgsMinusOneDecimalPointFloat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726f;
                return -1;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(870.000f));
    }

    @Test
    public void testApplyTwoArgsMinusTwoDecimalPointsFloat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726f;
                return -2;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(900.000f));
    }

    @Test
    public void testApplyTwoArgsMinusThreeDecimalPointsFloat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726f;
                return -3;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(1000.000f));
    }

    @Test
    public void testApplyTwoArgsMinusFourDecimalPointsFloat() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 873.726f;
                return -4;
            }
        };
        Object res = roundSqlFunction.apply(sqlFunctionExecutionContext);
        Assert.assertNotNull(res);
        Assert.assertTrue(res.equals(0.000f));
    }


    @Test(expected = RuntimeException.class)
    public void testApplyWrongTypeRightArg() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return "wow";
                return 1;
            }
        };
        roundSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test(expected = RuntimeException.class)
    public void testApplyWrongTypeLeftArg() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 872.333;
                return "wow";
            }
        };
        roundSqlFunction.apply(sqlFunctionExecutionContext);
    }
}