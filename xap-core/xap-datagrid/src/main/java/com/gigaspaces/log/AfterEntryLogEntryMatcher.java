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

/**
 * A matcher filter that will return log entries that happened from (after) the provided log entry.
 *
 * @author kimchy
 */

public class AfterEntryLogEntryMatcher extends LogEntryMatcherFilter {

    private static final long serialVersionUID = 1;

    private final long filePosition;

    private final long position;

    private final boolean inclusive;

    private transient long lastFilePosition;

    public AfterEntryLogEntryMatcher(LogEntries logEntries, LogEntry logEntry) {
        this(logEntries, logEntry, LogEntryMatchers.INCLUSIVE);
    }

    public AfterEntryLogEntryMatcher(LogEntries logEntries, LogEntry logEntry, boolean inclusive) {
        this(logEntries, logEntry, inclusive, new AllLogEntryMatcher());
    }

    public AfterEntryLogEntryMatcher(LogEntries logEntries, LogEntry logEntry, LogEntryMatcher matcher) {
        this(logEntries, logEntry, LogEntryMatchers.INCLUSIVE, matcher);
    }

    public AfterEntryLogEntryMatcher(LogEntries logEntries, LogEntry logEntry, boolean inclusive, LogEntryMatcher matcher) {
        super(matcher);
        LogEntry fileMarker = logEntries.findFileMarkerFor(logEntry);
        this.filePosition = fileMarker.getPosition();
        this.position = logEntry.getPosition();
        this.inclusive = inclusive;
    }

    @Override
    protected boolean filterJustLogs() {
        return false;
    }

    protected Operation filter(LogEntry entry) {
        if (entry.isFileMarker()) {
            lastFilePosition = entry.getPosition();
            if (entry.getPosition() >= filePosition) {
                return Operation.CONTINUE;
            } else {
                return Operation.BREAK;
            }
        }
        if (!entry.isLog()) {
            return Operation.CONTINUE;
        }
        // only filter when we are at the logEntry file, forward files should be included
        if (lastFilePosition != filePosition) {
            return Operation.CONTINUE;
        }
        if (inclusive && entry.getPosition() >= position) {
            return Operation.CONTINUE;
        }
        if (!inclusive && entry.getPosition() > position) {
            return Operation.CONTINUE;
        }
        return Operation.BREAK;
    }
}
