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

/**
 * A matcher filter that will filter out all log entries that do not contain the provided string.
 *
 * @author kimchy
 */

public class ContainsStringLogEntryMatcher extends LogEntryMatcherFilter {

    private static final long serialVersionUID = 1;

    private final String str;

    public ContainsStringLogEntryMatcher(String regex) {
        this(regex, new AllLogEntryMatcher());
    }

    public ContainsStringLogEntryMatcher(String str, LogEntryMatcher matcher) {
        super(matcher);
        this.str = str;
    }

    @Override
    public void initialize(InitializationContext context) throws IOException {
        super.initialize(context);
    }

    protected Operation filter(LogEntry entry) {
        if (entry.isLog() && entry.getText().contains(str)) {
            return Operation.CONTINUE;
        }
        return Operation.IGNORE;
    }
}