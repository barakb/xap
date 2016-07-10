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
import java.io.OutputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * This OutputStream sub class is used with the Blob implementation. NOTE: this class does not
 * handle synchronization!
 *
 * @author Michael Mitrani
 */
@com.gigaspaces.api.InternalApi
public class BlobOutputStream extends OutputStream {

    private Blob blob = null;
    private int startPosition = 0;

    public BlobOutputStream(Blob blob, int position) {
        this.blob = blob;
        this.startPosition = position;
    }

    /* (non-Javadoc)
     * @see java.io.OutputStream#close()
     */
    public void close() throws IOException {
        //nothing to do here
    }

    /* (non-Javadoc)
     * @see java.io.OutputStream#flush()
     */
    public void flush() throws IOException {
        //nothing to do here
    }

    public void write(byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }

    public void write(byte[] bytes, int off, int len) throws IOException {
        byte[] newarray = new byte[blob.blob.length + len];
        //if (startPosition != 0)
        //	System.arraycopy(blob.blob,0,newarray,0,startPosition);
        System.arraycopy(bytes, off, newarray, startPosition, len);
        //if (blob.blob.length > 0)
        //	System.arraycopy(blob.blob,startPosition,newarray,startPosition+len-1,blob.blob.length);
        blob.blob = newarray;
        updateSpace();
    }

    /* (non-Javadoc)
     * @see java.io.Writer#write(char[], int, int)
     */
    public void write(int c) throws IOException {
        write(new byte[]{(byte) c});
    }

    public void updateSpace() throws IOException {
        PreparedStatement ps = null;
        try {
            ps = blob.conn.prepareStatement("UPDATE BY UID SET LOB=?");
            ps.setBlob(1, this.blob);
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
