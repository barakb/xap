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

package org.openspaces.test.client.executor;

import java.text.DateFormat;
import java.util.Date;

/**
 * A logger running on the remote process. We can't use the java.util.logging since we run the risk
 * of overriding the any logging properties the remote process may have.
 *
 * @author moran
 */
public class ProcessLogger {


    private final static DateFormat timeFormatter = DateFormat.getTimeInstance();
    private final static DateFormat dateFormatter = DateFormat.getDateInstance();

    public static void log(String message) {
        doLog(message, null);
    }

    public static void log(String message, Throwable t) {
        doLog(message, t);
    }

    private static void doLog(String message, Throwable t) {

        if (t != null)
            message += " " + ExecutorUtils.getStackTrace(t);

        String caller = "";

        Date date = new Date();
        System.out.println("\n" + dateFormatter.format(date) + " - " + timeFormatter.format(date) + " [host: "
                + ExecutorUtils.getHostName() + "]" + caller + "\n  " + message);
    }
}
