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
import java.util.List;

/**
 * A matcher allowing to control the traversal of the log file. Also accumulates the required log
 * entries to be processed in order to return them.
 *
 * @author kimchy
 */
public interface LogEntryMatcher extends Serializable {

    enum Operation {
        BREAK,
        CONTINUE,
        IGNORE
    }

    class InitializationContext implements Serializable {
        private static final long serialVersionUID = 3526789181040829025L;

        final String hostAddress;
        final String hostName;
        final long pid;
        final LogProcessType processType;

        public InitializationContext(LogEntries entries) {
            this(entries.getHostAddress(), entries.getHostName(), entries.getPid(), entries.getProcessType());
        }

        public InitializationContext(String hostAddress, String hostName, long pid, LogProcessType processType) {
            this.hostAddress = hostAddress;
            this.hostName = hostName;
            this.pid = pid;
            this.processType = processType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            InitializationContext that = (InitializationContext) o;

            if (pid != that.pid) return false;
            if (hostAddress != null ? !hostAddress.equals(that.hostAddress) : that.hostAddress != null)
                return false;
            if (processType != that.processType) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = hostAddress != null ? hostAddress.hashCode() : 0;
            result = 31 * result + (hostName != null ? hostName.hashCode() : 0);
            result = 31 * result + (int) (pid ^ (pid >>> 32));
            result = 31 * result + (processType != null ? processType.hashCode() : 0);
            return result;
        }
    }

    /**
     * Called on the loggable component side (server) before starting to traverse the log file.
     */
    void initialize(InitializationContext context) throws IOException;

    /**
     * Returns all the relevant entries this matcher accumulated.
     *
     * <p>Note, for ease of use in implementing the matcher, the entries are assumed to be from
     * newest to oldest (if the matcher value order).
     */
    List<LogEntry> entries();

    /**
     * Controls if the traversal of the log file should continue, or break. If it should break, then
     * all the {@link #entries()} accumulated will be returned.
     */
    Operation match(LogEntry entry);
}
