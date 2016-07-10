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


/*
 * Created on 05/06/2004
 *
 * @author		Gershon Diner
 * Title:        	The GigaSpaces Platform
 * Copyright:   Copyright (c) GigaSpaces Team 2004
 * Company:    GigaSpaces Technologies Ltd.
 * @version 	4.0
 */
package com.j_spaces.jms;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;


/**
 * Implements the <code>javax.jms.BytesMessage</code> interface for the GigaSpaces JMS API. A
 * <CODE>BytesMessage</CODE> object is used to send a message containing a stream of uninterpreted
 * m_body_bytes. It inherits from the <CODE>Message</CODE> interface and adds a m_body_bytes message
 * body. The receiver of the message supplies the interpretation of the m_body_bytes.
 *
 * <P>The <CODE>BytesMessage</CODE> methods are based largely on those found in
 * <CODE>java.io.DataInputStream</CODE> and <CODE>java.io.DataOutputStream</CODE>.
 *
 * <P>This message type is for client encoding of existing message formats. If possible, one of the
 * other self-defining message types should be used instead.
 *
 * <P>Although the JMS API allows the use of message properties with byte messages, they are
 * typically not used, since the inclusion of properties may affect the format.
 *
 * <P>The primitive types can be written explicitly using methods for each type. They may also be
 * written generically as objects. For instance, a call to <CODE>BytesMessage.writeInt(6)</CODE> is
 * equivalent to <CODE>BytesMessage.writeObject(new Integer(6))</CODE>. Both forms are provided,
 * because the explicit form is convenient for static programming, and the object form is needed
 * when types are not known at compile time.
 *
 * <P>When the message is first created, and when <CODE>clearBody</CODE> is called, the body of the
 * message is in write-only mode. After the first call to <CODE>reset</CODE> has been made, the
 * message body is in read-only mode. After a message has been sent, the client that sent it can
 * retain and modify it without affecting the message that has been sent. The same message object
 * can be sent multiple times. When a message has been received, the provider has called
 * <CODE>reset</CODE> so that the message body is in read-only mode for the client.
 *
 * <P>If <CODE>clearBody</CODE> is called on a message in read-only mode, the message body is
 * cleared and the message is in write-only mode.
 *
 * <P>If a client attempts to read a message in write-only mode, a <CODE>MessageNotReadableException</CODE>
 * is thrown.
 *
 * <P>If a client attempts to write a message in read-only mode, a <CODE>MessageNotWriteableException</CODE>
 * is thrown.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 * @see Session#createBytesMessage()
 * @see MapMessage
 * @see Message
 * @see ObjectMessage
 * @see StreamMessage
 * @see TextMessage
 */
public class GSBytesMessageImpl
        extends GSMessageImpl
        implements BytesMessage {

    private static final long serialVersionUID = 1L;

    /**
     * The array in which the written data is buffered.
     */
    private ByteArrayOutputStream bytesOut;
    /**
     * The stream in which body data is written.
     */
    private DataOutputStream dataOut;
    /**
     * The stream for reading the written data.
     */
    private DataInputStream dataIn;

    /** <code>true</code> if the message has been sent since its last modif.
     private boolean prepared;// = false;*/


    /**
     * Instantiates a new <code>GSBytesMessageImpl</code>.
     *
     * @throws JMSException if falied to create the message
     */
    public GSBytesMessageImpl() throws JMSException {
        super();
    }

    /**
     * Instantiates a new <code>GSBytesMessageImpl</code>.
     *
     * @param session    the session of the message
     * @param bytesArray the bytes content of the message
     * @throws JMSException if falied to create the message
     */
    public GSBytesMessageImpl(GSSessionImpl session, byte[] bytesArray)
            throws JMSException {
        super(session, GSMessageImpl.BYTES);
        Body = bytesArray;
    }

    /**
     * Instantiates a new <code>GSBytesMessageImpl</code>.
     *
     * @param session the session of the message
     * @throws JMSException if falied to create the message
     */
    public GSBytesMessageImpl(GSSessionImpl session)
            throws JMSException {
        this(session, null);
    }

    /**
     * Sets the bytes array.
     *
     * @param bytesArray the bytes array
     */
    public void setBytes(byte[] bytesArray) {
        Body = bytesArray;
    }

    /**
     * Returns the bytes array.
     *
     * @return the bytes array
     */
    public byte[] getBytes() {
        return (byte[]) Body;
    }


    public void seal() throws IOException {
        if (this.dataOut != null) {
            this.dataOut.flush();
            Body = this.bytesOut.toByteArray();
            dataOut.close();
            bytesOut.close();
            dataOut = null;
            bytesOut = null;
        }
    }


    /**
     * Gets the number of bytes of the message body when the message is in read-only mode. The value
     * returned can be used to allocate a byte array. The value returned is the entire length of the
     * message body, regardless of where the pointer for reading the message is currently located.
     *
     * @return number of bytes in the message
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageNotReadableException if the message is in write-only mode.
     * @since 1.1
     */
    public long getBodyLength() throws JMSException {
        checkBodyReadOnly();
        return ((byte[]) Body).length;
    }

    /**
     * Reads a <code>boolean</code> from the bytes message stream.
     *
     * @return the <code>boolean</code> value read
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public boolean readBoolean() throws JMSException {
        initializeReading();
        try {
            this.dataIn.mark(Integer.MAX_VALUE);
            return this.dataIn.readBoolean();
        } catch (EOFException eof) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageEOFException(eof.toString());
            jmsEx.setLinkedException(eof);
            throw jmsEx;
        } catch (IOException ioe) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageFormatException("Format error occurred" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Reads a signed 8-bit value from the bytes message stream.
     *
     * @return the next byte from the bytes message stream as a signed 8-bit <code>byte</code>
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public byte readByte() throws JMSException {
        initializeReading();
        try {
            this.dataIn.mark(Integer.MAX_VALUE);
            return this.dataIn.readByte();
        } catch (EOFException eof) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageEOFException(eof.toString());
            jmsEx.setLinkedException(eof);
            throw jmsEx;
        } catch (IOException ioe) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageFormatException("Format error occurred" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Reads an unsigned 8-bit number from the bytes message stream.
     *
     * @return the next byte from the bytes message stream, interpreted as an unsigned 8-bit number
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public int readUnsignedByte() throws JMSException {
        initializeReading();
        try {
            this.dataIn.mark(Integer.MAX_VALUE);
            return this.dataIn.readUnsignedByte();
        } catch (EOFException eof) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageEOFException(eof.toString());
            jmsEx.setLinkedException(eof);
            throw jmsEx;
        } catch (IOException ioe) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageFormatException("Format error occurred" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Reads a signed 16-bit number from the bytes message stream.
     *
     * @return the next two bytes from the bytes message stream, interpreted as a signed 16-bit
     * number
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public short readShort() throws JMSException {
        initializeReading();
        try {
            this.dataIn.mark(Integer.MAX_VALUE);
            return this.dataIn.readShort();
        } catch (EOFException eof) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageEOFException(eof.toString());
            jmsEx.setLinkedException(eof);
            throw jmsEx;
        } catch (IOException ioe) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageFormatException("Format error occurred" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Reads an unsigned 16-bit number from the bytes message stream.
     *
     * @return the next two bytes from the bytes message stream, interpreted as an unsigned 16-bit
     * integer
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public int readUnsignedShort() throws JMSException {
        initializeReading();
        try {
            this.dataIn.mark(Integer.MAX_VALUE);
            return this.dataIn.readUnsignedShort();
        } catch (EOFException eof) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageEOFException(eof.toString());
            jmsEx.setLinkedException(eof);
            throw jmsEx;
        } catch (IOException ioe) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageFormatException("Format error occurred" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Reads a Unicode character value from the bytes message stream.
     *
     * @return the next two bytes from the bytes message stream as a Unicode character
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public char readChar() throws JMSException {
        initializeReading();
        try {
            this.dataIn.mark(Integer.MAX_VALUE);
            return this.dataIn.readChar();
        } catch (EOFException eof) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageEOFException(eof.toString());
            jmsEx.setLinkedException(eof);
            throw jmsEx;
        } catch (IOException ioe) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageFormatException("Format error occurred" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Reads a signed 32-bit integer from the bytes message stream.
     *
     * @return the next four bytes from the bytes message stream, interpreted as an <code>int</code>
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public int readInt() throws JMSException {
        initializeReading();
        try {
            this.dataIn.mark(Integer.MAX_VALUE);
            return this.dataIn.readInt();
        } catch (EOFException eof) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageEOFException(eof.toString());
            jmsEx.setLinkedException(eof);
            throw jmsEx;
        } catch (IOException ioe) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageFormatException("Format error occurred" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Reads a signed 64-bit integer from the bytes message stream.
     *
     * @return the next eight bytes from the bytes message stream, interpreted as a
     * <code>long</code>
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public long readLong() throws JMSException {
        initializeReading();
        try {
            this.dataIn.mark(Integer.MAX_VALUE);
            return this.dataIn.readLong();
        } catch (EOFException eof) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageEOFException(eof.toString());
            jmsEx.setLinkedException(eof);
            throw jmsEx;
        } catch (IOException ioe) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageFormatException("Format error occurred" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Reads a <code>float</code> from the bytes message stream.
     *
     * @return the next four bytes from the bytes message stream, interpreted as a
     * <code>float</code>
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public float readFloat() throws JMSException {
        initializeReading();
        try {
            this.dataIn.mark(Integer.MAX_VALUE);
            return this.dataIn.readFloat();
        } catch (EOFException eof) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageEOFException(eof.toString());
            jmsEx.setLinkedException(eof);
            throw jmsEx;
        } catch (IOException ioe) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageFormatException("Format error occurred" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Reads a <code>double</code> from the bytes message stream.
     *
     * @return the next eight bytes from the bytes message stream, interpreted as a
     * <code>double</code>
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public double readDouble() throws JMSException {
        initializeReading();
        try {
            this.dataIn.mark(Integer.MAX_VALUE);
            return this.dataIn.readDouble();
        } catch (EOFException eof) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageEOFException(eof.toString());
            jmsEx.setLinkedException(eof);
            throw jmsEx;
        } catch (IOException ioe) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageFormatException("Format error occurred" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Reads a string that has been encoded using a modified UTF-8 format from the bytes message
     * stream. <P> For more information on the UTF-8 format, see "File System Safe UCS
     * Transformation Format (FSS_UTF)", X/Open Preliminary Specification, X/Open Company Ltd.,
     * Document Number: P316. This information also appears in ISO/IEC 10646, Annex P.
     *
     * @return a Unicode string from the bytes message stream
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public String readUTF() throws JMSException {
        initializeReading();
        try {
            this.dataIn.mark(Integer.MAX_VALUE);
            return this.dataIn.readUTF();
        } catch (EOFException eof) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageEOFException(eof.toString());
            jmsEx.setLinkedException(eof);
            throw jmsEx;
        } catch (IOException ioe) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageFormatException("Format error occurred" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Reads a byte array from the bytes message stream. <P> If the length of array
     * <code>value</code> is less than the number of bytes remaining to be read from the stream, the
     * array should be filled. A subsequent call reads the next increment, and so on. <P> If the
     * number of bytes remaining in the stream is less than the length of array <code>value</code>,
     * the bytes should be read into the array. The return value of the total number of bytes read
     * will be less than the length of the array, indicating that there are no more bytes left to be
     * read from the stream. The next read of the stream returns -1.
     *
     * @param value the buffer into which the data is read
     * @return the total number of bytes read into the buffer, or -1 if there is no more data
     * because the end of the stream has been reached
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public int readBytes(byte[] value) throws JMSException {
        return readBytes(value, value.length);
    }

    /**
     * Reads a portion of the bytes message stream. <P> If the length of array <code>value</code> is
     * less than the number of bytes remaining to be read from the stream, the array should be
     * filled. A subsequent call reads the next increment, and so on. <P> If the number of bytes
     * remaining in the stream is less than the length of array <code>value</code>, the bytes should
     * be read into the array. The return value of the total number of bytes read will be less than
     * the length of the array, indicating that there are no more bytes left to be read from the
     * stream. The next read of the stream returns -1. <p/> If <code>length</code> is negative, or
     * <code>length</code> is greater than the length of the array <code>value</code>, then an
     * <code>IndexOutOfBoundsException</code> is thrown. No bytes will be read from the stream for
     * this exception case.
     *
     * @param value  the buffer into which the data is read
     * @param length the number of bytes to read; must be less than or equal to
     *               <code>value.length</code>
     * @return the total number of bytes read into the buffer, or -1 if there is no more data
     * because the end of the stream has been reached
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    public int readBytes(byte[] value, int length) throws JMSException {
        initializeReading();
        if (value == null) {
            throw new NullPointerException("Byte buffer is null");
        }
        if (length < 0 || length > value.length) {
            throw new IndexOutOfBoundsException("Illegal byte buffer size. Must be between 0 and value.length.");
        }
        this.dataIn.mark(Integer.MAX_VALUE);
        try {
            int n = 0;
            while (n < length) {
                int count = this.dataIn.read(value, n, length - n);
                if (count < 0) {
                    break;
                }
                n += count;
            }
            if (n == 0 && length > 0) {
                n = -1;
            }
            return n;
        } catch (EOFException eof) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageEOFException(eof.toString());
            jmsEx.setLinkedException(eof);
            throw jmsEx;
        } catch (IOException ioe) {
            try {
                this.dataIn.reset();
            } catch (IOException e) {
                JMSException jmsEx = new JMSException(e.toString());
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
            JMSException jmsEx = new MessageFormatException("Format error occurred" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Writes a <code>boolean</code> to the bytes message stream as a 1-byte value. The value
     * <code>true</code> is written as the value <code>(byte)1</code>; the value <code>false</code>
     * is written as the value <code>(byte)0</code>.
     *
     * @param value the <code>boolean</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void writeBoolean(boolean value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeBoolean(value);
        } catch (IOException ioe) {
            JMSException jmsEx = new JMSException("Could not write data:" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Writes a <code>byte</code> to the bytes message stream as a 1-byte value.
     *
     * @param value the <code>byte</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void writeByte(byte value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeByte(value);
        } catch (IOException ioe) {
            JMSException jmsEx = new JMSException("Could not write data:" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Writes a <code>short</code> to the bytes message stream as two bytes, high byte first.
     *
     * @param value the <code>short</code> to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void writeShort(short value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeShort(value);
        } catch (IOException ioe) {
            JMSException jmsEx = new JMSException("Could not write data:" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Writes a <code>char</code> to the bytes message stream as a 2-byte value, high byte first.
     *
     * @param value the <code>char</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void writeChar(char value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeChar(value);
        } catch (IOException ioe) {
            JMSException jmsEx = new JMSException("Could not write data:" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Writes an <code>int</code> to the bytes message stream as four bytes, high byte first.
     *
     * @param value the <code>int</code> to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void writeInt(int value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeInt(value);
        } catch (IOException ioe) {
            JMSException jmsEx = new JMSException("Could not write data:" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Writes a <code>long</code> to the bytes message stream as eight bytes, high byte first.
     *
     * @param value the <code>long</code> to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void writeLong(long value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeLong(value);
        } catch (IOException ioe) {
            JMSException jmsEx = new JMSException("Could not write data:" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Converts the <code>float</code> argument to an <code>int</code> using the
     * <code>floatToIntBits</code> method in class <code>Float</code>, and then writes that
     * <code>int</code> value to the bytes message stream as a 4-byte quantity, high byte first.
     *
     * @param value the <code>float</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void writeFloat(float value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeFloat(value);
        } catch (IOException ioe) {
            JMSException jmsEx = new JMSException("Could not write data:" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Converts the <code>double</code> argument to a <code>long</code> using the
     * <code>doubleToLongBits</code> method in class <code>Double</code>, and then writes that
     * <code>long</code> value to the bytes message stream as an 8-byte quantity, high byte first.
     *
     * @param value the <code>double</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void writeDouble(double value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeDouble(value);
        } catch (IOException ioe) {
            JMSException jmsEx = new JMSException("Could not write data:" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Writes a string to the bytes message stream using UTF-8 encoding in a machine-independent
     * manner. <P> For more information on the UTF-8 format, see "File System Safe UCS
     * Transformation Format (FSS_UTF)", X/Open Preliminary Specification, X/Open Company Ltd.,
     * Document Number: P316. This information also appears in ISO/IEC 10646, Annex P.
     *
     * @param value the <code>String</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void writeUTF(String value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeUTF(value);
        } catch (IOException ioe) {
            JMSException jmsEx = new JMSException("Could not write data:" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Writes a byte array to the bytes message stream.
     *
     * @param value the byte array to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void writeBytes(byte[] value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(value);
        } catch (IOException ioe) {
            JMSException jmsEx = new JMSException("Could not write data:" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Writes a portion of a byte array to the bytes message stream.
     *
     * @param value  the byte array value to be written
     * @param offset the initial offset within the byte array
     * @param length the number of bytes to use
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(value, offset, length);
        } catch (IOException ioe) {
            JMSException jmsEx = new JMSException("Could not write data:" + ioe.toString());
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * Writes an object to the bytes message stream. <P> This method works only for the objectified
     * primitive object types (<code>Integer</code>,<code>Double</code>, <code>Long</code>
     * &nbsp;...), <code>String</code> objects, and byte arrays.
     *
     * @param value the object in the Java programming language ("Java object") to be written; it
     *              must not be null
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageFormatException       if the object is of an invalid type.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     * @throws NullPointerException         if the parameter <code>value</code> is null.
     */
    public void writeObject(Object value) throws JMSException {
        if (value == null) {
            throw new NullPointerException();
        }
        initializeWriting();
        if (value instanceof Boolean) {
            writeBoolean(((Boolean) value).booleanValue());
        } else if (value instanceof Character) {
            writeChar(((Character) value).charValue());
        } else if (value instanceof Byte) {
            writeByte(((Byte) value).byteValue());
        } else if (value instanceof Short) {
            writeShort(((Short) value).shortValue());
        } else if (value instanceof Integer) {
            writeInt(((Integer) value).intValue());
        } else if (value instanceof Double) {
            writeDouble(((Double) value).doubleValue());
        } else if (value instanceof Long) {
            writeLong(((Long) value).longValue());
        } else if (value instanceof Float) {
            writeFloat(((Float) value).floatValue());
        } else if (value instanceof Double) {
            writeDouble(((Double) value).doubleValue());
        } else if (value instanceof String) {
            writeUTF(value.toString());
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        } else {
            throw new MessageFormatException("Cannot write non-primitive type:" + value.getClass());
        }
    }

    /**
     * Puts the message body in read-only mode and repositions the stream of bytes to the
     * beginning.
     *
     * @throws JMSException if an internal error occurs
     */
    public void reset() throws JMSException {
        if (this.dataOut != null) {
            try {
                this.dataOut.flush();
                Body = this.bytesOut.toByteArray();
                dataOut.close();
                bytesOut.close();
            } catch (IOException ioe) {
                JMSException jmsEx = new JMSException("reset failed: " + ioe.toString());
                jmsEx.setLinkedException(ioe);
                throw jmsEx;
            }
        }
        if (this.dataIn != null) {
            try {
                this.dataIn.close();
            } catch (IOException ioe) {
                JMSException jmsEx = new JMSException("reset failed: " + ioe.toString());
                jmsEx.setLinkedException(ioe);
                throw jmsEx;
            }
        }
        this.setBodyReadOnly(true);
        this.bytesOut = null;
        this.dataIn = null;
        this.dataOut = null;
    }

    private void initializeWriting() throws MessageNotWriteableException {
        checkBodyReadOnly();
        if (this.dataOut == null) {
            this.bytesOut = new ByteArrayOutputStream();
            this.dataOut = new DataOutputStream(this.bytesOut);
        }
    }

    private void initializeReading() throws MessageNotReadableException {
        checkBodyWriteOnly();
        if (this.dataIn == null) {
            byte[] body;
            if (Body == null) {
                body = new byte[0];
                Body = body;
            } else {
                body = (byte[]) Body;
            }
            ByteArrayInputStream bytesIn = new ByteArrayInputStream(body, 0, body.length);
            this.dataIn = new DataInputStream(bytesIn);
        }
    }

    /**
     * Overide the super class method to reset the streams, and put the message body in write only
     * mode.
     *
     * <p>If <code>clearBody</code> is called on a message in read-only mode, the message body is
     * cleared and the message is in write-only mode. bytes to the beginning.
     *
     * <p>If <code>clearBody</code> is called on a message already in write-only mode, the spec does
     * not define the outcome, so do nothing. Client must then call <code>reset</code>, followed by
     * <code>clearBody</code> to reset the stream at the beginning for a new write.
     *
     * @throws JMSException if JMS fails to reset the message due to some internal JMS error
     */
    public void clearBody() throws JMSException {
        Body = new byte[0];
        setBodyReadOnly(false);
        try {
            if (dataOut != null) {
                dataOut.close();
            }
            if (dataIn != null) {
                dataIn.close();
            }
            if (bytesOut != null) {
                bytesOut.close();
            }
        } catch (IOException e) {
            JMSException jmsEx = new JMSException(
                    "IOException while clearing message body: " + e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } finally {
            this.dataOut = null;
            this.dataIn = null;
            this.bytesOut = null;
        }
    }


    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("\n\n\tBytesMessage:");
        sb.append("\n\t\tbytesOut:\t\t");
        sb.append(bytesOut);
        sb.append("\n\t\tdataIn:\t\t\t");
        sb.append(dataIn);
        sb.append("\n\t\tdataOut:\t\t");
        sb.append(dataOut);
        return sb.toString();
    }


    /**
     * Creates a copy of this message.
     *
     * @return the copy of this message.
     */
    GSMessageImpl duplicate() throws JMSException {
        GSBytesMessageImpl dup = new GSBytesMessageImpl();
        copyTo(dup);
        return dup;
    }


    /**
     * Returns a clone of the body.
     *
     * @return a clone of the body.
     */
    protected Object cloneBody() {
        if (Body == null) {
            return null;
        }
        byte[] body = (byte[]) Body;
        byte[] cloned = new byte[body.length];
        System.arraycopy(body, 0, cloned, 0, body.length);
        return cloned;
    }

}//end of class
