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

/**
 * Created by Barak Bar Orion on 1/20/16.
 *
 * @since 11.0
 */
public abstract class SqlFunction {
    public abstract Object apply(SqlFunctionExecutionContext context);

    protected void assertNumberOfArguments(int expected, SqlFunctionExecutionContext context) {
        if (context.getNumberOfArguments() != expected) {
            throw new RuntimeException("wrong number of arguments - expected: " + expected + " ,but actual number of arguments is: " + context.getNumberOfArguments());
        }
    }
}
