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
 * Built in mathematical sql function to get absolute value of a parameter.
 *
 * @author Tamir Schwarz
 * @since 11.0.0
 */
@com.gigaspaces.api.InternalApi
public class AbsSqlFunction extends SqlFunction {
    /**
     * @param context contains one parameter of type Number.
     * @return the absolute value of context.getArgument(0), cast to context.getArgument(0).getClass().
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(1, context);
        Object arg = context.getArgument(0);
        if (arg instanceof Integer) {
            return Math.abs((Integer) arg);
        } else if (arg instanceof Long) {
            return Math.abs((Long) arg);
        } else if (arg instanceof Float) {
            return Math.abs((Float) arg);
        } else if (arg instanceof Double) {
            return Math.abs((Double) arg);
        } else if (arg instanceof Short) {
            Integer abs = Math.abs((Short) arg);
            return abs.shortValue();
        } else if (arg instanceof Byte) {
            Integer abs = Math.abs((Byte) arg);
            return abs.byteValue();
        } else {
            throw new RuntimeException("Abs function - wrong argument type: " + arg);
        }
    }
}
