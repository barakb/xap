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

import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * This writer sub class is used with the Clob implementation. NOTE: this class does not handle
 * synchronization!
 *
 * @author Michael Mitrani
 */
@com.gigaspaces.api.InternalApi
public class ClobWriter extends Writer {

    final private Clob clob;

    public ClobWriter(Clob clob, int position) {
        this.clob = clob;
    }

    /* (non-Javadoc)
     * @see java.io.Writer#close()
     */
    public void close() throws IOException {
        //nothing to do here
    }

    /* (non-Javadoc)
     * @see java.io.Writer#flush()
     */
    public void flush() throws IOException {
        //nothing to do here
    }

    /**
     * Characters are inserted into the clob based on the position given of the clob. The emphasis
     * is on insert rather than overwrite.
     */
    public void write(char[] cbuf) throws IOException {
        writeIntoClob(new String(cbuf));
    }

    public void write(int c) throws IOException {
        writeIntoClob(new String(new char[]{(char) c}));
    }

    public void write(String str, int off, int len) throws IOException {
        writeIntoClob(str.substring(off, off + len));
    }

    /* (non-Javadoc)
     * @see java.io.Writer#write(char[], int, int)
     */
    public void write(char[] cbuf, int off, int len) throws IOException {
        writeIntoClob(new String(cbuf, off, len));
    }

    public void write(String str) throws IOException {
        writeIntoClob(str);
    }

    private void writeIntoClob(String newString) throws IOException {
        //String prefix = clob.clob.substring(0,startPosition);
        //String suffix = clob.clob.substring(startPosition, clob.clob.length());
        //StringBuffer buffer = new StringBuffer(prefix);
        //buffer.append(newString);
        //buffer.append(suffix);
        //clob.clob = buffer.toString();
        clob.clob = newString;
        updateSpace();
    }

    private void updateSpace() throws IOException {
        PreparedStatement ps = null;
        try {
            ps = clob.conn.prepareStatement("UPDATE BY UID SET LOB=?");
            ps.setClob(1, this.clob);
            ps.executeUpdate();
        } catch (SQLException sqle) {
            throw new IOException(sqle.getMessage());
        } finally {
            try {
                if (ps != null) ps.close();
            } catch (SQLException sqle) {
            }
        }
    }

}
