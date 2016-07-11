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
 * Built in string sql function to get the position of the first occurrence of a substring in a
 * string.
 *
 * @author Tamir Schwarz
 * @since 11.0.0
 */
@com.gigaspaces.api.InternalApi
public class InStrSqlFunction extends SqlFunction {
    /**
     * inStr is an SQL convention (Not Java's), in the sense that the first index is 1 and not 0.
     *
     * @param context contains two arguments of type string.
     * @return the index of the first occurrence of context.getArgument(1) in
     * context.getArgument(0). If not a substring, return value of 0.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object str = context.getArgument(0);
        Object subStr = context.getArgument(1);
        if (str != null && subStr != null && str instanceof String && subStr instanceof String) {
            return String.valueOf(str).indexOf(String.valueOf(subStr)) + 1;
        } else {
            throw new RuntimeException("InStr function - wrong arguments types. First argument:[" + str + "]. Second argument:[ " + subStr + "]");
        }
    }
}
