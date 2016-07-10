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

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by Tamir on 2/21/16.
 *
 * @since 11.0.0s
 */
@com.gigaspaces.api.InternalApi
public class ToCharSqlFunction extends SqlFunction {
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        Object arg = context.getArgument(0);
        Object format = null;
        if (context.getNumberOfArguments() >= 2) {
            format = context.getArgument(1);
        }
        if (arg instanceof Number) { //number
            if (format != null) { // must be java- DecimalFormat
                DecimalFormat decimalFormat = new DecimalFormat(String.valueOf(format));
                return decimalFormat.format(arg);
            } else {
                return arg;
            }
        } else if (arg instanceof Date) { // Note - the Time zone will always evaluated as GMT
            if (format != null) { // must be java- SimpleDateFormat
                SimpleDateFormat sdf = new SimpleDateFormat(String.valueOf(format));
                sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
                return sdf.format(arg);
            }
            return arg;
        }
        throw new RuntimeException("To_Char function - wrong argument type: " + arg);
    }
}
