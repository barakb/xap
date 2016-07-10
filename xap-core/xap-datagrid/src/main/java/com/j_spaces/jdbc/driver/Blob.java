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


package com.j_spaces.jdbc.driver;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * The java.sql.Blob implementation
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class Blob extends ObjectWithUID implements java.sql.Blob, Comparable {

    private static final long serialVersionUID = 7084515297198345838L;

    protected byte[] blob;
    protected transient Connection conn;

    public Blob(byte[] blob, Connection conn) throws SQLException {
        if (blob == null)
            throw new SQLException("Blob cannot hold null data", "GSP", -131);
        this.blob = blob;
        this.conn = conn;
    }

    public Blob(byte[] blob) {
        this.blob = blob;
    }

    public long length() throws SQLException {
        return blob.length;
    }

    public void truncate(long len) throws SQLException {
        throw new SQLException("Command not supported!", "GSP", -132);
    }

    public byte[] getBytes(long pos, int length) throws SQLException {
        if (pos < 1 || length < 1)
            throw new SQLException("Both parameters should be greater than 1", "GSP", -133);
        if (pos + length > blob.length + 1)
            length = (blob.length - (int) pos + 1);
        byte[] newarray = new byte[length];
        System.arraycopy(blob, (int) pos - 1, newarray, 0, length);
        return newarray;
    }

    public int setBytes(long pos, byte[] bytes) throws SQLException {
        throw new SQLException("Command not supported!", "GSP", -132);
    }

    public int setBytes(long pos, byte[] bytes, int offset, int len)
            throws SQLException {
        throw new SQLException("Command not supported!", "GSP", -132);
    }

    public long position(byte[] pattern, long start) throws SQLException {
        boolean found = false;
        while (!found) {
            if (pattern.length + start > blob.length + 1)
                return -1L;
            found = true;
            for (int i = 0; i <= pattern.length; i++) {
                if (pattern[i] != blob[i + (int) start - 1]) {
                    found = false;
                    break;
                }
            }
            if (!found)
                start++;
        }
        return start;
    }

    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(blob);
    }

    public OutputStream getBinaryOutputStream() throws SQLException {
        return setBinaryStream(0);
    }

    public OutputStream setBinaryStream(long pos) throws SQLException {
        return new BlobOutputStream(this, (int) pos);
    }

    public long position(java.sql.Blob pattern, long start) throws SQLException {
        com.j_spaces.jdbc.driver.Blob gBlob = (com.j_spaces.jdbc.driver.Blob) pattern;
        return this.position(gBlob.blob, start);
    }

    public boolean equals(Object ob) {
        if (!(ob instanceof Blob))
            return false;
        Blob oBlob = (Blob) ob;
        if (oBlob.blob.length != this.blob.length)
            return false;
        for (int i = 0; i < this.blob.length; i++) {
            if (this.blob[i] != oBlob.blob[i])
                return false;
        }
        return true;
    }

    public int hashCode() {
        int sum = 0;
        for (int i = 0; i < blob.length; i++) {
            sum += blob[i];
        }
        return sum;
    }

    public int compareTo(Object other) throws ClassCastException {
        return (this.hashCode() - ((Blob) other).hashCode());
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder("BLOB[");
        for (int i = 0; i < blob.length; i++) {
            buffer.append(blob[i]);
            buffer.append(',');
        }
        buffer.deleteCharAt(buffer.length() - 1);
        buffer.append(']');
        return buffer.toString();
    }

    public void setConnection(Connection conn) {
        this.conn = conn;
    }

    public void free() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(long pos, long length)
            throws SQLException {
        throw new UnsupportedOperationException();
    }
}
