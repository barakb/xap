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
public class ConcatSqlFunctionTest {

    private ConcatSqlFunction concatSqlFunction;

    @Before
    public void setUp() throws Exception {
        concatSqlFunction = new ConcatSqlFunction();
    }

    @Test
    public void testApplySingleString() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return "ab";
            }
        };

        Object res = concatSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        String s = "ab";
        assertTrue(s.equals(res));
    }

    @Test
    public void testApplyStringWithNull() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return "ab";
                return null;
            }
        };

        Object res = concatSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        String s = "abnull";
        assertTrue(s.equals(res));
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
                if (index == 0)
                    return "ab";
                return "cd";
            }
        };

        Object res = concatSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        String s = "abcd";
        assertTrue(s.equals(res));
    }

    @Test
    public void testApplyTwoStringsWithSpaceInTheMiddle() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 3;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return "ab";
                if (index == 1)
                    return " ";
                return "cd";
            }
        };

        Object res = concatSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        String s = "ab cd";
        assertTrue(s.equals(res));
    }

    @Test
    public void testApplyStringWithInteger() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return "ab";
                return 1;
            }
        };

        Object res = concatSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        String s = "ab1";
        assertTrue(s.equals(res));
    }

    @Test
    public void testApplyStringWithUserDefinedPojo() throws Exception {

        class Person {
            private String _name;
            private Integer _age;

            Person(String name, Integer age) {
                _name = name;
                _age = age;
            }

            @Override
            public String toString() {
                return _name + " is " + _age + " years old";
            }
        }

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return "Person ";
                return new Person("Yosi", 5);
            }
        };

        Object res = concatSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        String s = "Person Yosi is 5 years old";
        assertTrue(s.equals(res));
    }
}