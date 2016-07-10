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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Holds a list of {@link com.gigaspaces.log.LogEntries}. Note, the list might include
 * <code>null</code> values, so use {@link #getSafeEntries()} in order to get a list that includes
 * all non null values.
 *
 * @author kimchy
 */

public class CompoundLogEntries implements Serializable {

    private static final long serialVersionUID = 1;

    private final LogEntries[] entries;

    private transient LogEntries[] safeEntries;

    /**
     * Constructs a new instance with an array of {@link com.gigaspaces.log.LogEntries} (some values
     * can be null within the array).
     */
    public CompoundLogEntries(LogEntries[] entries) {
        this.entries = entries;
    }

    /**
     * Returns the {@link com.gigaspaces.log.LogEntries} array.
     *
     * <p>Note, some values of the array might be null. Use {@link #getSafeEntries()} in order to
     * get all the non null log entries.
     */
    public LogEntries[] getEntries() {
        return entries;
    }

    /**
     * Returns a {@link com.gigaspaces.log.LogEntries} array with no null values.
     */
    public LogEntries[] getSafeEntries() {
        if (safeEntries != null) {
            return safeEntries;
        }
        List<LogEntries> result = new ArrayList<LogEntries>(entries.length);
        for (LogEntries x : entries) {
            if (x != null) {
                result.add(x);
            }
        }
        safeEntries = result.toArray(new LogEntries[result.size()]);
        return safeEntries;
    }

    /**
     * Returns <code>true</code> if there are no log entries.
     */
    public boolean isEmpty() {
        if (entries == null || entries.length == 0) {
            return true;
        }
        for (LogEntries x : entries) {
            if (x != null) {
                return false;
            }
        }
        return true;
    }
}
