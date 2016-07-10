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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A collection of log entries, including meta information such as process id, host information.
 *
 * @author kimchy
 */

public class LogEntries implements Iterable<LogEntry>, Externalizable {

    private static final long serialVersionUID = 1;

    private LogProcessType processType;

    private List<LogEntry> entries;

    private int totalLogFiles;

    private long pid;

    private long latestTimestamp;

    private String hostName = "";

    private String hostAddress = "";

    private transient List<LogEntry> justLogEntries;

    /**
     * Default constructor for Externalizable readExternal usage
     */
    public LogEntries() {
    }

    /**
     * Empty LogEntries
     */
    public LogEntries(LogProcessType processType, long pid, String hostName, String hostAddress) {
        this(processType, new ArrayList<LogEntry>(0), 0, pid, 0, hostName, hostAddress);
    }

    public LogEntries(LogProcessType processType, List<LogEntry> entries, int totalLogFiles, long pid, long latestTimestamp, String hostName, String hostAddress) {
        this.processType = processType;
        this.entries = entries;
        this.totalLogFiles = totalLogFiles;
        this.pid = pid;
        this.latestTimestamp = latestTimestamp;
        this.hostName = hostName;
        this.hostAddress = hostAddress;
    }

    /**
     * The process type that generated the logs.
     */
    public LogProcessType getProcessType() {
        return processType;
    }

    /**
     * The list of log entries.
     */
    public List<LogEntry> getEntries() {
        return entries;
    }

    /**
     * The process id of the process responsible for writing the log entries.
     */
    public long getPid() {
        return this.pid;
    }

    /**
     * Ths host name of the process responsible for writing the log entries.
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Ths host name of the process responsible for writing the log entries.
     */
    public String getHostAddress() {
        return hostAddress;
    }

    /**
     * The latest timestamp of change happened to the log file.
     */
    public long getLatestLogFileTimestamp() {
        return latestTimestamp;
    }

    /**
     * Returns the total number of log files scanned.
     */
    public int getTotalLogFiles() {
        return totalLogFiles;
    }

    public Iterator<LogEntry> iterator() {
        return entries.iterator();
    }

    /**
     * Returns a list of all the {@link com.gigaspaces.log.LogEntry} which are of the log type
     * (represent an actual log line).
     */
    public List<LogEntry> logEntries() {
        return typeEntries(LogEntry.Type.LOG);
    }

    /**
     * Returns a list of all the {@link com.gigaspaces.log.LogEntry} which are of the prvided type.
     */
    public List<LogEntry> typeEntries(LogEntry.Type type) {
        if (type == LogEntry.Type.LOG && justLogEntries != null) {
            return justLogEntries;
        }
        // fugly, we can implement our own
        ArrayList<LogEntry> result = new ArrayList<LogEntry>();
        for (LogEntry entry : entries) {
            if (entry.getType() == type) {
                result.add(entry);
            }
        }
        if (type == LogEntry.Type.LOG) {
            justLogEntries = result;
        }
        return result;
    }

    /**
     * Finds the log entry of type {@link com.gigaspaces.log.LogEntry.Type#FILE_MARKER} that
     * corresponds the the provided log entry. Basically, allows to get information about the actual
     * file from where this log entry was extracted.
     */
    public LogEntry findFileMarkerFor(LogEntry logEntry) {
        if (logEntry.getType() == LogEntry.Type.FILE_MARKER) {
            return logEntry;
        }
        int index = entries.indexOf(logEntry);
        if (index == -1) {
            throw new IllegalArgumentException("Log entry not found in entries");
        }
        //go from first element for retrieving latest file descriptor from the
        //beginning of list because list is already reversed
        for (int i = 0; i < index; i++) {
            LogEntry candidate = entries.get(i);
            if (candidate.getType() == LogEntry.Type.FILE_MARKER) {
                return candidate;
            }
        }
        throw new IllegalStateException("No file marker found in entries");
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(processType);
        out.writeInt(totalLogFiles);
        out.writeLong(pid);
        out.writeLong(latestTimestamp);
        out.writeUTF(hostName);
        out.writeUTF(hostAddress);
        out.writeInt(entries.size());
        for (LogEntry entry : entries) {
            entry.writeExternal(out);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        processType = (LogProcessType) in.readObject();
        totalLogFiles = in.readInt();
        pid = in.readLong();
        latestTimestamp = in.readLong();
        hostName = in.readUTF();
        hostAddress = in.readUTF();
        int size = in.readInt();
        entries = new ArrayList<LogEntry>(size);
        for (int i = 0; i < size; i++) {
            LogEntry entry = new LogEntry();
            entry.readExternal(in);
            entries.add(entry);
        }
    }
}
