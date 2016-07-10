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

import java.util.ArrayList;
import java.util.List;

/**
 * A matcher that returns only stores the last N entries but does not break (basically, will return
 * the last N elements processed).
 *
 * @author kimchy
 */

public class SizeLogEntryMatcher implements LogEntryMatcher {

    private static final long serialVersionUID = 1;

    private int size;

    private transient LogEntry[] entries;

    private transient int index;

    private transient LogEntry lastFileMarker;

    public SizeLogEntryMatcher(int size) {
        this.size = size;
    }

    public void initialize(InitializationContext context) {
        entries = new LogEntry[size];
    }

    public List<LogEntry> entries() {
        ArrayList<LogEntry> result = new ArrayList<LogEntry>();
        for (int i = index; i < size; i++) {
            if (entries[i] == null) {
                return result;
            }
            result.add(entries[i]);
        }
        for (int i = 0; i < index; i++) {
            if (entries[i] == null) {
                return result;
            }
            result.add(entries[i]);
        }
        // check if we have the last file marker, if not, add it
        boolean found = false;
        for (LogEntry entry : entries) {
            if (entry == lastFileMarker) {
                found = true;
                break;
            }
        }
        if (!found) {
            result.add(lastFileMarker);
        }
        return result;
    }

    public Operation match(LogEntry entry) {
        if (entry.isFileMarker()) {
            lastFileMarker = entry;
        }
        entries[index++] = entry;
        if (index == size) {
            index = 0;
        }
        return Operation.CONTINUE;
    }
}