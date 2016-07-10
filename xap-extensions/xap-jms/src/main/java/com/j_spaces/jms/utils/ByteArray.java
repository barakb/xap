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

package com.j_spaces.jms.utils;

import java.io.Serializable;

/**
 * Simple holder for a an array of Bytes - used instead of a ByteBuffer to avoid unecessary
 * System.array() copies
 *
 * @author Gershon Diner
 * @version 1.0
 * @since 5.1
 */
public class ByteArray implements Serializable {
    /** */
    private static final long serialVersionUID = 1L;
    private byte[] buf;
    private int offset;
    private int length;

    /**
     * Construct an empty ByteArray
     */
    public ByteArray() {
    }

    /**
     * Create a byte array
     */
    public ByteArray(byte[] buf) {
        this(buf, 0, buf.length);
    }

    /**
     * Create a ByteArray
     */
    public ByteArray(byte[] buf, int offset, int length) {
        this.buf = buf;
        this.offset = offset;
        this.length = length;
    }

    /**
     * clear the values held by this ByteArray
     */
    public void clear() {
        buf = null;
        offset = 0;
        length = 0;
    }

    /**
     * reset values
     */
    public void reset(byte[] buf) {
        if (buf != null) {
            reset(buf, 0, buf.length);
        } else {
            clear();
        }
    }

    /**
     * reset values
     */
    public void reset(byte[] buf, int offset, int length) {
        this.buf = buf;
        this.offset = offset;
        this.length = length;
    }

    /**
     * @return Returns the buf.
     */
    public byte[] getBuf() {
        return buf;
    }

    /**
     * @param buf The buf to set.
     */
    public void setBuf(byte[] buf) {
        this.buf = buf;
    }

    /**
     * @return Returns the length.
     */
    public int getLength() {
        return length;
    }

    /**
     * @param length The length to set.
     */
    public void setLength(int length) {
        this.length = length;
    }

    /**
     * @return Returns the offset.
     */
    public int getOffset() {
        return offset;
    }

    /**
     * @param offset The offset to set.
     */
    public void setOffset(int offset) {
        this.offset = offset;
    }

    /**
     * @return the byte at the position
     */
    public byte get(int position) {
        return buf[offset + position];
    }

    /**
     * make a copy
     *
     * @return a copy of it's self
     */
    public ByteArray copy() {
        ByteArray result = new ByteArray();
        if (buf != null) {
            byte[] data = new byte[length];
            System.arraycopy(buf, offset, data, 0, length);
            result.reset(data);
        }
        return result;
    }
}