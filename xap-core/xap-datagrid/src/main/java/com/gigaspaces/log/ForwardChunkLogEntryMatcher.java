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
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A forward chunk that iterates over all the log files, from start to end. Returning
 * <code>null</code> if there is nothing more to iterate over.
 *
 * @author kimchy
 */

public class ForwardChunkLogEntryMatcher implements LogEntryMatcher, ClientLogEntryMatcherCallback, StreamLogEntryMatcher {

    private static final long serialVersionUID = 1;

    private final LogEntryMatcher origMatcher;

    private final Map<InitializationContext, Holder> matchers;

    private transient LogEntryMatcher serverSideMatcher;

    public ForwardChunkLogEntryMatcher(LogEntryMatcher matcher) {
        this.origMatcher = matcher;
        this.matchers = new HashMap<InitializationContext, Holder>();
    }

    public void initialize(InitializationContext context) throws IOException {
        Holder holder = matchers.get(context);
        if (holder == null) {
            holder = new Holder(new FirstFileLogEntryMatcher(origMatcher), 0);
            matchers.put(context, holder);
        }
        serverSideMatcher = holder.matcher;
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
        Holder holder = matchers.get(context);
        if (holder == null) {
            holder = new Holder(new FirstFileLogEntryMatcher(origMatcher), 0);
            matchers.put(context, holder);
        }
        if (holder.lastFilePosition == entries.getTotalLogFiles()) {
            return null;
        }
        if (holder.matcher instanceof ClientLogEntryMatcherCallback) {
            entries = ((ClientLogEntryMatcherCallback) holder.matcher).clientSideProcess(entries);
        }
        List<LogEntry> logs = entries.logEntries();
        if (!logs.isEmpty()) {
            LogEntry lastEntry = logs.get(logs.size() - 1);
            holder.matcher = new SameFileLogEntryMatcher(holder.lastFilePosition, new AfterEntryLogEntryMatcher(entries, lastEntry, origMatcher));
            return entries;
        }
        // no logs entries anymore, move to the next file
        holder.lastFilePosition++;
        if (holder.lastFilePosition == entries.getTotalLogFiles()) {
            holder.matcher = new NoneLogEntryMatcher();
            return null;
        }
        holder.matcher = new SameFileLogEntryMatcher(holder.lastFilePosition, origMatcher);
        return entries;
    }

    private static class Holder implements Serializable {
        private static final long serialVersionUID = -2077404952794343627L;

        LogEntryMatcher matcher;
        long lastFilePosition;

        private Holder(LogEntryMatcher matcher, long lastFilePosition) {
            this.matcher = matcher;
            this.lastFilePosition = lastFilePosition;
        }
    }
}
