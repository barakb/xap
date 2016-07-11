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
 * Built in mathematical sql function to perform modulo operation.
 *
 * @author Tamir Schwarz
 * @since 11.0.0
 */
@com.gigaspaces.api.InternalApi
public class ModSqlFunction extends SqlFunction {
    /**
     * @param context contains two arguments of either Long/Integer/Double.
     * @return the remainder of context.getArgument(0) divided by context.getArgument(1).
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object leftNum = context.getArgument(0);
        Object rightNum = context.getArgument(1);
        if (!(leftNum instanceof Number) || !(rightNum instanceof Number)) { // fail fast
            throw new RuntimeException("Mod function - wrong arguments types, both arguments should be Double, Long or Integer. First argument:[" + leftNum + "]. Second argument:[ " + rightNum + "]");
        }
        if (leftNum instanceof Integer && rightNum instanceof Integer) {
            return Integer.valueOf(String.valueOf(leftNum)) % Integer.valueOf(String.valueOf(rightNum));
        } else if (leftNum instanceof Double && rightNum instanceof Double) {
            return Double.valueOf(String.valueOf(leftNum)) % Double.valueOf(String.valueOf(rightNum));
        } else if (leftNum instanceof Integer && rightNum instanceof Double) {
            return Integer.valueOf(String.valueOf(leftNum)) % Double.valueOf(String.valueOf(rightNum));
        } else if (leftNum instanceof Double && rightNum instanceof Integer) {
            return Double.valueOf(String.valueOf(leftNum)) % Integer.valueOf(String.valueOf(rightNum));
        } else if (leftNum instanceof Double && rightNum instanceof Long) {
            return Double.valueOf(String.valueOf(leftNum)) % Long.valueOf(String.valueOf(rightNum));
        } else if (leftNum instanceof Long && rightNum instanceof Double) {
            return Long.valueOf(String.valueOf(leftNum)) % Double.valueOf(String.valueOf(rightNum));
        } else if (leftNum instanceof Long && rightNum instanceof Integer) {
            return Long.valueOf(String.valueOf(leftNum)) % Integer.valueOf(String.valueOf(rightNum));
        } else if (leftNum instanceof Integer && rightNum instanceof Long) {
            return Integer.valueOf(String.valueOf(leftNum)) % Long.valueOf(String.valueOf(rightNum));
        } else if (leftNum instanceof Long && rightNum instanceof Long) {
            return Long.valueOf(String.valueOf(leftNum)) % Long.valueOf(String.valueOf(rightNum));
        } else {
            throw new RuntimeException("Mod function - wrong arguments types, both arguments should be Double, Long or Integer. First argument:[" + leftNum + "]. Second argument:[ " + rightNum + "]");
        }
    }
}
