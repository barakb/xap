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
 * Created by Tamir on 2/21/16.
 *
 * @since 11.0.0
 */
@com.gigaspaces.api.InternalApi
public class FloorSqlFunction extends SqlFunction {
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(1, context);
        Object arg = context.getArgument(0);
        if (!(arg instanceof Number)) {// fast fail
            throw new RuntimeException("Floor function - wrong argument type, should be a Number - : " + arg);
        }
        if (arg instanceof Double) {
            return Math.floor((Double) arg);
        } else if (arg instanceof Float) {
            return (float) Math.floor((Float) arg);
        } else if (arg instanceof Long) {
            return arg;
        } else if (arg instanceof Integer) {
            return arg;
        } else if (arg instanceof Short) {
            return arg;
        } else if (arg instanceof Byte) {
            return arg;
        } else {
            throw new RuntimeException("Floor function - wrong argument type - unsupported Number type: " + arg);
        }
    }
}
