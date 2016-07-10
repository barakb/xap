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
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * The java.sql.Clob implementation.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class Clob extends ObjectWithUID implements java.sql.Clob, Comparable {

    static final long serialVersionUID = 8586407571272951992L;

    protected String clob;
    protected transient Connection conn;

    public Clob(String clob, Connection conn) throws SQLException {
        if (clob == null)
            throw new SQLException("Clob cannot hold null data", "GSP", -134);
        this.clob = clob;
        this.conn = conn;
    }

    public Clob(String clob) {
        this.clob = clob;
    }

    public String getClobData() {
        return clob;
    }

    public long length() throws SQLException {
        return clob.length();
    }

    public void truncate(long len) throws SQLException {
        throw new SQLException("Command not supported!", "GSP", -132);
    }

    public InputStream getAsciiStream() throws SQLException {
        try {
            byte[] asciiBytes = clob.getBytes("ASCII");
            return new ByteArrayInputStream(asciiBytes);
        } catch (UnsupportedEncodingException uee) {
            throw new SQLException("Can't convert clob to ASCII, unsupported encoding",
                    "GSP", -135);
        }
    }

    public OutputStream setAsciiStream(long pos) throws SQLException {
        throw new SQLException("Command  not supported!", "GSP", -132);
    }

    public Reader getCharacterStream() throws SQLException {
        return new StringReader(clob);
    }

    public Writer setCharacterStream(long pos) throws SQLException {
        return new ClobWriter(this, (int) pos);
    }

    public Writer getCharacterOutputStream() throws SQLException {
        return setCharacterStream(0);
    }

    public String getSubString(long pos, int length) throws SQLException {
        int p = (int) pos - 1;
        if (p < 0 || length < 1 || p + length > clob.length())
            throw new SQLException("Substring out of clob's bounds", "GSP", -136);

        return clob.substring(p, p + length);
    }

    public int setString(long pos, String str) throws SQLException {
        throw new SQLException("Command not supported!", "GSP", -132);
    }

    public int setString(long pos, String str, int offset, int len)
            throws SQLException {
        throw new SQLException("Command not supported!", "GSP", -132);
    }

    public long position(String searchstr, long start) throws SQLException {
        for (int i = (int) start - 1; i < clob.length(); i++) {
            if (clob.regionMatches(i, searchstr, 0, searchstr.length()))
                return i;
        }
        return -1;
    }

    public long position(java.sql.Clob searchstr, long start)
            throws SQLException {
        com.j_spaces.jdbc.driver.Clob gClob = (com.j_spaces.jdbc.driver.Clob) searchstr;
        return this.position(gClob.clob, start);
    }

    @Override
    public boolean equals(Object ob) {
        if (!(ob instanceof Clob))
            return false;
        Clob oClob = (Clob) ob;
        return (oClob.clob.equals(this.clob));
    }

    @Override
    public int hashCode() {
        return clob.hashCode();
    }

    public int compareTo(Object other) throws ClassCastException {
        if (other instanceof Clob)
            return (clob.compareTo(((Clob) other).clob));
        else return -1;
    }

    public String toString() {
        return "CLOB[" + clob + "]";
    }

    public void setConnection(Connection conn) {
        this.conn = conn;
    }

    public void free() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(long pos, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

}
