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
 * A streaming log entry matcher that allows to traverse the log entries in a streaming manner in a
 * reverse order.
 *
 * @author kimchy
 */

public class ReverseLogEntryMatcher implements LogEntryMatcher, ClientLogEntryMatcherCallback, StreamLogEntryMatcher {

    private static final long serialVersionUID = 1;

    private transient LogEntryMatcher origMatcher;

    private LogEntryMatcher matcher;

    public ReverseLogEntryMatcher(LogEntryMatcher matcher) {
        this.origMatcher = matcher;
        this.matcher = matcher;
    }

    public void initialize(InitializationContext context) throws IOException {
        matcher.initialize(context);
    }

    public List<LogEntry> entries() {
        return matcher.entries();
    }

    public Operation match(LogEntry entry) {
        return matcher.match(entry);
    }

    public LogEntries clientSideProcess(LogEntries entries) {
        if (matcher instanceof ClientLogEntryMatcherCallback) {
            entries = ((ClientLogEntryMatcherCallback) matcher).clientSideProcess(entries);
        }
        List<LogEntry> justLogs = entries.logEntries();
        if (justLogs.size() > 0) {
            LogEntry lastEntry = justLogs.get(0);
            matcher = new BeforeEntryLogEntryMatcher(entries, lastEntry, origMatcher);
        }
        return entries;
    }
}