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
import java.util.List;

/**
 * A base class for entry matchers that act as filters to a delegated matcher.
 *
 * @author kimchy
 */
public abstract class LogEntryMatcherFilter implements LogEntryMatcher {
    private static final long serialVersionUID = -5843948024474262714L;

    private final LogEntryMatcher matcher;

    protected LogEntryMatcherFilter(LogEntryMatcher matcher) {
        this.matcher = matcher;
    }

    public void initialize(InitializationContext context) throws IOException {
        matcher.initialize(context);
    }

    public List<LogEntry> entries() {
        return matcher.entries();
    }

    public Operation match(LogEntry entry) {
        Operation op = doFilter(this, entry);
        if (op != null) {
            return op;
        }
        return matcher.match(entry);
    }

    /**
     * Should this filter only filter log type entries. Defaults to <code>true</code>.
     */
    protected boolean filterJustLogs() {
        return true;
    }

    /**
     * Should the operation be filtered or not. {@link LogEntryMatcher.Operation#BREAK} in order to
     * break and finish the matching process. {@link com.gigaspaces.log.LogEntryMatcher.Operation#IGNORE}
     * to ignore the current log entry (so it won't be passed to the delegated matcher). And {@link
     * com.gigaspaces.log.LogEntryMatcher.Operation#CONTINUE} to pass the current log entry to the
     * delegated matcher.
     */
    protected abstract Operation filter(LogEntry entry);

    Operation doFilter(LogEntryMatcherFilter filter, LogEntry entry) {
        if (!filter.filterJustLogs() || (filter.filterJustLogs() && entry.isLog())) {
            Operation op = filter(entry);
            if (op == Operation.BREAK) {
                return Operation.BREAK;
            }
            if (op == Operation.IGNORE) {
                // if we ignore this line, we simply return continue to the next line
                return Operation.CONTINUE;
            }
        }
        return null;
    }
}
