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
package net.jini.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This class is an optimized porting of java.io.ByteArrayOutputStream: a) All methods are not
 * synchronized. b) Most safety checks have been removed. c) ensureCapacity method have been added,
 * for low level optimizations. Naturally, this class and all of its methods are not thread safe.
 *
 * @author niv
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class OptimizedByteArrayOutputStream extends OutputStream {
    /**
     * The buffer where data is stored.
     */
    protected byte[] _buffer;

    /**
     * The number of valid bytes in the buffer.
     */
    protected int _count;

    /**
     * Creates a new byte array output stream. The buffer capacity is initially 32 bytes, though its
     * size increases if necessary.
     */
    public OptimizedByteArrayOutputStream() {
        this(32);
    }

    /**
     * Creates a new byte array output stream, with a buffer capacity of the specified size, in
     * bytes.
     *
     * @param capacity the initial capacity.
     * @throws IllegalArgumentException if size is negative.
     */
    public OptimizedByteArrayOutputStream(int capacity) {
        if (capacity < 0)
            throw new IllegalArgumentException("Negative initial size: " + capacity);
        _buffer = new byte[capacity];
    }

    /**
     * Copy internal buffer
     */
    final public void copyToBuffer(ByteBuffer buffer) {
        buffer.put(_buffer, 0, _count);
    }

    /**
     * Set stream buffer, and reset the counter.
     *
     * @param buf new buffer
     */
    public void setBuffer(byte[] buf) {
        setBuffer(buf, 0);
    }

    /**
     * Set stream buffer and set the counter.
     *
     * @param count  amount of valid bytes
     * @param buffer stream buffer
     */
    public void setBuffer(byte[] buffer, int count) {
        this._buffer = buffer;
        this._count = count;
    }

    /**
     * Gets internal buffers
     *
     * @return internal buffer
     */
    public byte[] getBuffer() {
        return this._buffer;
    }

    /**
     * Set the buffer size.
     *
     * @param size amount of valid bytes
     */
    public void setSize(int size) {
        this._count = size;
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return the value of the <code>count</code> field, which is the number of valid bytes in this
     * output stream.
     * @see java.io.ByteArrayOutputStream#count
     */
    public int size() {
        return _count;
    }

    /**
     * The current buffer capacity.
     *
     * @return buffer current capacity
     */
    public int getCapacity() {
        return _buffer.length;
    }

    /**
     * Writes the specified byte to this byte array output stream.
     *
     * @param b the byte to be written.
     */
    public void write(int b) {
        ensureCapacity(1);
        _buffer[_count++] = (byte) b;
    }

    public void writeByte(byte b) {
        ensureCapacity(1);
        _buffer[_count++] = b;
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array starting at offset
     * <code>off</code> to this byte array output stream.
     *
     * @param b   the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     */
    public void write(byte b[], int off, int len) {
        if (len == 0)
            return;
        ensureCapacity(len);
        System.arraycopy(b, off, _buffer, _count, len);
        _count += len;
    }

    public boolean ensureCapacity(int delta) {
        int newcount = _count + delta;
        if (newcount > _buffer.length) {
            byte newbuf[] = new byte[Math.max(_buffer.length << 1, newcount)];
            System.arraycopy(_buffer, 0, newbuf, 0, _count);
            _buffer = newbuf;
            return true;
        }
        return false;
    }

    /**
     * Writes the complete contents of this byte array output stream to the specified output stream
     * argument, as if by calling the output stream's write method using <code>out.write(buf, 0,
     * count)</code>.
     *
     * @param out the output stream to which to write the data.
     * @throws java.io.IOException if an I/O error occurs.
     */
    public void writeTo(OutputStream out) throws IOException {
        out.write(_buffer, 0, _count);
    }

    /**
     * Resets the <code>count</code> field of this byte array output stream to zero, so that all
     * currently accumulated output in the output stream is discarded. The output stream can be used
     * again, reusing the already allocated buffer space.
     *
     * @see java.io.ByteArrayInputStream#count
     */
    public void reset() {
        _count = 0;
    }

    /**
     * Creates a newly allocated byte array. Its size is the current size of this output stream and
     * the valid contents of the buffer have been copied into it.
     *
     * @return the current contents of this output stream, as a byte array.
     * @see java.io.ByteArrayOutputStream#size()
     */
    public byte[] toByteArray() {
        byte newbuf[] = new byte[_count];
        System.arraycopy(_buffer, 0, newbuf, 0, _count);
        return newbuf;
    }
}