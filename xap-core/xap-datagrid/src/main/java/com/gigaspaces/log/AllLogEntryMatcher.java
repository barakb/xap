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
 * A matcher that returns all the log entries. <b>Use with caution, as this might lead to heavy
 * memory usage without wrapping it with a proper filter.</b>
 *
 * @author kimchy
 */

public class AllLogEntryMatcher implements LogEntryMatcher {

    private static final long serialVersionUID = 1;

    private transient ArrayList<LogEntry> entries;

    public void initialize(InitializationContext context) {
        entries = new ArrayList<LogEntry>();
    }

    public List<LogEntry> entries() {
        return entries;
    }

    public Operation match(LogEntry entry) {
        entries.add(entry);
        return Operation.CONTINUE;
    }
}
