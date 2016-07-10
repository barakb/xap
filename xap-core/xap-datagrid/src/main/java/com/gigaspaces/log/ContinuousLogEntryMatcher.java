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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A reusable matcher (not thread safe) which accepts a matcher that returns the log entries for the
 * first call, and for any other call, a {@link AfterEntryLogEntryMatcher} will be used with the
 * last log entry returned.
 *
 * @author kimchy
 */

public class ContinuousLogEntryMatcher implements LogEntryMatcher, ClientLogEntryMatcherCallback, StreamLogEntryMatcher {

    private static final long serialVersionUID = 1;

    private final LogEntryMatcher initialMatcher;

    private transient LogEntryMatcher continousMatcher;

    private final Map<InitializationContext, LogEntryMatcher> matchers;

    private transient LogEntryMatcher serverSideMatcher;

    public ContinuousLogEntryMatcher(LogEntryMatcher initialMatcher, LogEntryMatcher continousMatcher) {
        this.initialMatcher = initialMatcher;
        this.continousMatcher = continousMatcher;
        this.matchers = new HashMap<InitializationContext, LogEntryMatcher>();
    }

    public void initialize(InitializationContext context) throws IOException {
        LogEntryMatcher matcher = matchers.get(context);
        if (matcher == null) {
            matcher = initialMatcher;
            matchers.put(context, matcher);
        }
        serverSideMatcher = matcher;
        serverSideMatcher.initialize(context);
    }

    public List<LogEntry> entries() {
        return serverSideMatcher.entries();
    }

    public Operation match(LogEntry entry) {
        return serverSideMatcher.match(entry);
    }

    public LogEntries clientSideProcess(LogEntries entries) {
        InitializationContext context = new InitializationContext(entries);
        LogEntryMatcher matcher = matchers.get(context);
        if (matcher == null) {
            matcher = initialMatcher;
            matchers.put(context, matcher);
        }
        if (matcher instanceof ClientLogEntryMatcherCallback) {
            entries = ((ClientLogEntryMatcherCallback) matcher).clientSideProcess(entries);
        }
        List<LogEntry> justLogs = entries.logEntries();
        if (!justLogs.isEmpty()) {
            LogEntry lastEntry = justLogs.get(justLogs.size() - 1);
            if (continousMatcher != null) {
                matcher = new AfterEntryLogEntryMatcher(entries, lastEntry, continousMatcher);
            } else {
                matcher = new AfterEntryLogEntryMatcher(entries, lastEntry);
            }
            matchers.put(context, matcher);
        }
        return entries;
    }
}
