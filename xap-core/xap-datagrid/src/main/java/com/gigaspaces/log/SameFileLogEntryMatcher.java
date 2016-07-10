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
 * A log entry matcher filter that matches on logs that only exists within the same log file.
 *
 * @author kimchy
 */

public class SameFileLogEntryMatcher extends LogEntryMatcherFilter {

    private static final long serialVersionUID = 1;

    private final long filePosition;

    public SameFileLogEntryMatcher(LogEntries logEntries, LogEntry logEntry) {
        this(logEntries, logEntry, new AllLogEntryMatcher());
    }

    public SameFileLogEntryMatcher(LogEntries logEntries, LogEntry logEntry, LogEntryMatcher matcher) {
        super(matcher);
        this.filePosition = logEntries.findFileMarkerFor(logEntry).getPosition();
    }

    public SameFileLogEntryMatcher(long filePosition) {
        this(filePosition, new AllLogEntryMatcher());
    }

    public SameFileLogEntryMatcher(long filePosition, LogEntryMatcher matcher) {
        super(matcher);
        this.filePosition = filePosition;
    }

    @Override
    protected boolean filterJustLogs() {
        return false;
    }

    protected Operation filter(LogEntry entry) {
        if (entry.isFileMarker()) {
            if (entry.getPosition() > filePosition) {
                return Operation.IGNORE;
            }
            if (entry.getPosition() < filePosition) {
                return Operation.BREAK;
            }
        }
        return Operation.CONTINUE;
    }
}
