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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Represents a log entry.
 *
 * @author kimchy
 */

public class LogEntry implements Externalizable {

    private static final long serialVersionUID = 1;

    private static String lineSeparator = System.getProperty("line.separator");

    /**
     * The type of the log entry. Two types are provided, an entry representing an actual log "line"
     * and a file marker representing log file.
     */
    public static enum Type {
        LOG((byte) 0),
        FILE_MARKER((byte) 1);

        byte value;

        Type(byte value) {
            this.value = value;
        }

        public static Type fromByte(byte b) {
            if (b == 0) {
                return LOG;
            } else if (b == 1) {
                return FILE_MARKER;
            }
            throw new IllegalArgumentException("Wrong type [" + b + "]");
        }
    }

    private long position;

    private Type type;

    private long timestamp;

    private String text;

    public LogEntry() {
    }

    public LogEntry(long position, Type type, long timestamp, String text) {
        this.position = position;
        this.type = type;
        this.timestamp = timestamp;
        this.text = text;
    }

    /**
     * Returns the timestamp this log was taken at.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the position of the log entry. Mainly for internal use.
     */
    public long getPosition() {
        return position;
    }

    /**
     * Returns the full log text.
     */
    public String getText() {
        return text;
    }

    /**
     * Returns the full log text with line separator.
     */
    public String getTextWithLF() {
        return getText() + lineSeparator;
    }

    /**
     * Returns the type of the log entry.
     */
    public Type getType() {
        return type;
    }

    /**
     * Returns <code>true</code> if the log entry is an actual log "line".
     */
    public boolean isLog() {
        return type == Type.LOG;
    }

    /**
     * Returns <code>true</code> if the log entry is a file marker (representing the file where the
     * log was extracted).
     */
    public boolean isFileMarker() {
        return type == Type.FILE_MARKER;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(position);
        out.writeByte(type.value);
        out.writeLong(timestamp);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_6_1))
            IOUtils.writeString(out, text);
        else
            out.writeUTF(text);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        position = in.readLong();
        type = Type.fromByte(in.readByte());
        timestamp = in.readLong();
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_6_1))
            text = IOUtils.readString(in);
        else
            text = in.readUTF();
    }
}
