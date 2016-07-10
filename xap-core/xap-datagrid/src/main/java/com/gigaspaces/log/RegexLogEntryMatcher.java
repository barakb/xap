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


package com.gigaspaces.log;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A log entry matcher filter that will match on log entries which match on the given regular
 * expression.
 *
 * @author kimchy
 */

public class RegexLogEntryMatcher extends LogEntryMatcherFilter {

    private static final long serialVersionUID = 1;

    private final String regex;

    private transient Matcher matcher;

    public static int regexFlags() {
        return Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE | Pattern.UNIX_LINES;
    }

    public RegexLogEntryMatcher(String regex) {
        this(regex, new AllLogEntryMatcher());
    }

    public RegexLogEntryMatcher(String regex, LogEntryMatcher matcher) {
        super(matcher);
        this.regex = regex;
    }

    @Override
    public void initialize(InitializationContext context) throws IOException {
        super.initialize(context);
        matcher = Pattern.compile(regex, regexFlags()).matcher("");
    }

    protected Operation filter(LogEntry entry) {
        if (matcher.reset(entry.getText()).matches()) {
            return Operation.CONTINUE;
        }
        return Operation.IGNORE;
    }
}
