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
 * Built in mathematical sql function to round the argument to nearest Integer.
 *
 * @author Tamir Schwarz
 * @since 11.0.0
 */
@com.gigaspaces.api.InternalApi
public class RoundSqlFunction extends SqlFunction {
    /**
     * Rounds the first argument context.getArgument(0) to the number of decimal places specified by
     * context.getArgument(1). <ul> <li>ROUND(1.298, 1) = 1.3</li> <li>ROUND(1.298, 0) = 1</li>
     * <li>ROUND(23.298, -1) = 20</li> </ul>
     *
     * @param context contains one or two arguments of type Number.
     * @return the value of the rounded argument context.getArgument(0) to context.getArgument(1)
     * decimal places. The rounding algorithm depends on the data type of context.getArgument(0),
     * context.getArgument(1) defaults to 0 if not specified. context.getArgument(1) can be negative
     * to cause context.getArgument(1) digits left of the decimal point of the value
     * context.getArgument(0) to become zero.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        Object arg = context.getArgument(0);
        Integer decimalPlaces = 0;

        if (context.getNumberOfArguments() >= 2) {
            decimalPlaces = (Integer) context.getArgument(1);
        }

        if (!(arg instanceof Number)) {
            throw new RuntimeException("Round function - wrong argument type, should be type Number - : " + arg);
        }

        double factor = Math.pow(10, decimalPlaces);

        if (arg instanceof Double) {
            long res = Math.round((Double) arg * factor);
            return res / factor;

        } else if (arg instanceof Float) {
            long res = Math.round((Float) arg * factor);
            return new Double(res / factor).floatValue();
        } else { // might be Integer, Long, Short, Byte
            return arg;
        }
    }
}
