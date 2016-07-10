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


package com.gigaspaces.logger;

import com.gigaspaces.lrmi.LRMIInvocationContext;

import org.jini.rio.boot.LoggableClassLoader;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Print a brief summary of the LogRecord in a human readable messageFormat. This class is a
 * variation of {@link java.util.logging.SimpleFormatter}, that was customized for GigaSpaces
 * purposes.
 *
 * @author Alex Beresnev
 * @version 5.1
 */

public class GSSimpleFormatter extends Formatter {

    final static int DATE_TIME = 0;
    final static int CLASS_NAME = 1;
    final static int METHOD_NAME = 2;
    final static int LEVEL = 3;
    final static int LOGGER_NAME = 4;
    final static int MESSAGE = 5;
    final static int CONTEXT = 6;
    final static int THREAD_NAME = 7;
    final static int THREAD_ID = 8;
    final static int LRMI_INVOCATION_SHORT_CONTEXT = 9;
    final static int LRMI_INVOCATION_LONG_CONTEXT = 10;
    final static int lastIndex = LRMI_INVOCATION_LONG_CONTEXT + 1;

    private final static String defaultPattern = "{0,date,yyyy-MM-dd HH:mm:ss,SSS} {6} {3} [{4}] - {5}";
    private final MessageFormat messageFormat;
    private final boolean[] patternIds = new boolean[lastIndex];

    // Line separator string.  This is the value of the line.separator
    // property at the moment that the SimpleFormatter was created.
    private final static String lineSeparator = System.getProperty("line.separator");

    private final Object _args[] = new Object[lastIndex];
    private final Date _date = new Date();

    public GSSimpleFormatter() {
        this(getDefinedPattern());
    }

    public GSSimpleFormatter(String pattern) {
        messageFormat = new MessageFormat(pattern);
        for (int i = 0; i < lastIndex; ++i) {
            if (pattern.contains(String.valueOf("{" + i + "}")) || pattern.contains(String.valueOf("{" + i + ","))) {
                patternIds[i] = true;
            }
        }
    }

    private static final String getDefinedPattern() {
        LogManager manager = LogManager.getLogManager();
        String pattern = manager.getProperty(GSSimpleFormatter.class.getName() + ".format");
        if (pattern == null) {
            return defaultPattern;
        } else {
            return pattern;
        }
    }

    /**
     * Format the given LogRecord.
     *
     * @param record the log record to be formatted.
     * @return a formatted log record
     */
    @Override
    public synchronized String format(LogRecord record) {
        StringBuffer text = new StringBuffer();
        setArgsWithRecordData(record);
        messageFormat.format(_args, text, null);

        // print stack trace if it exists and log level doesn't equal INFO
        Throwable thrown = record.getThrown();
        if (thrown != null) {
            //exceptions are logged only at a loggable level of the Record
            Logger exceptionLogger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_EXCEPTIONS);
            if (exceptionLogger.isLoggable(record.getLevel()) || thrown instanceof RuntimeException) {
                try {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    thrown.printStackTrace(pw);
                    pw.close();
                    text.append("; Caused by: ").append(sw.toString());
                } catch (Exception ex) {
                    text.append("; Caused by: ").append(record.getThrown().toString());
                    text.append(" - Unable to parse stack trace; Caught: ").append(ex);
                }
            } else {
                text.append("; Caused by: ").append(record.getThrown().toString());
            }
        }

        text.append(lineSeparator);
        return text.toString();
    }

    private void setArgsWithRecordData(LogRecord record) {

        if (patternIds[DATE_TIME]) {
            _date.setTime(record.getMillis());
            _args[DATE_TIME] = _date;
        }

        if (patternIds[CLASS_NAME]) {
            _args[CLASS_NAME] = record.getSourceClassName();
            if (_args[CLASS_NAME] == null)
                _args[CLASS_NAME] = "";
        }

        if (patternIds[METHOD_NAME]) {
            _args[METHOD_NAME] = record.getSourceMethodName();
            if (_args[METHOD_NAME] == null)
                _args[METHOD_NAME] = "";
        }

        if (patternIds[LEVEL])
            _args[LEVEL] = record.getLevel().getName();

        if (patternIds[LOGGER_NAME])
            _args[LOGGER_NAME] = record.getLoggerName();

        if (patternIds[MESSAGE])
            _args[MESSAGE] = formatMessage(record);

        if (patternIds[CONTEXT])
            _args[CONTEXT] = findContext();

        if (patternIds[THREAD_NAME])
            _args[THREAD_NAME] = Thread.currentThread().getName();

        if (patternIds[THREAD_ID])
            _args[THREAD_ID] = record.getThreadID();

        if (patternIds[LRMI_INVOCATION_SHORT_CONTEXT])
            _args[LRMI_INVOCATION_SHORT_CONTEXT] = LRMIInvocationContext.getContextMethodShortDisplayString();

        if (patternIds[LRMI_INVOCATION_LONG_CONTEXT])
            _args[LRMI_INVOCATION_LONG_CONTEXT] = LRMIInvocationContext.getContextMethodLongDisplayString();
    }

    private String findContext() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            return "null";
        }
        if (classLoader instanceof LoggableClassLoader) {
            return ((LoggableClassLoader) classLoader).getLogName();
        }
        classLoader = classLoader.getParent();
        if (classLoader instanceof LoggableClassLoader) {
            return ((LoggableClassLoader) classLoader).getLogName();
        }
        return "";
    }
}
