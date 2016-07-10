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

import com.j_spaces.jms.utils.ByteArray;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.StreamMessage;


/**
 * Implements the <code>javax.jms.StreamMessage</code> interface for the GigaSpaces JMS API.
 *
 * A <CODE>StreamMessage</CODE> object is used to send a stream of primitive types in the Java
 * programming language. It is filled and read sequentially. It inherits from the
 * <CODE>Message</CODE> interface and adds a stream message body. Its methods are based largely on
 * those found in <CODE>java.io.DataInputStream</CODE> and <CODE>java.io.DataOutputStream</CODE>.
 * <p/> <P>The primitive types can be read or written explicitly using methods for each type. They
 * may also be read or written generically as objects. For instance, a call to
 * <CODE>StreamMessage.writeInt(6)</CODE> is equivalent to <CODE>StreamMessage.writeObject(new
 * Integer(6))</CODE>. Both forms are provided, because the explicit form is convenient for static
 * programming, and the object form is needed when types are not known at compile time. <p/> <P>When
 * the message is first created, and when <CODE>clearBody</CODE> is called, the body of the message
 * is in write-only mode. After the first call to <CODE>reset</CODE> has been made, the message body
 * is in read-only mode. After a message has been sent, the client that sent it can retain and
 * modify it without affecting the message that has been sent. The same message object can be sent
 * multiple times. When a message has been received, the provider has called <CODE>reset</CODE> so
 * that the message body is in read-only mode for the client. <p/> <P>If <CODE>clearBody</CODE> is
 * called on a message in read-only mode, the message body is cleared and the message body is in
 * write-only mode. <p/> <P>If a client attempts to read a message in write-only mode, a
 * <CODE>MessageNotReadableException</CODE> is thrown. <p/> <P>If a client attempts to write a
 * message in read-only mode, a <CODE>MessageNotWriteableException</CODE> is thrown. <p/>
 * <P><CODE>StreamMessage</CODE> objects support the following conversion table. The marked cases
 * must be supported. The unmarked cases must throw a <CODE>JMSException</CODE>. The
 * <CODE>String</CODE>-to-primitive conversions may throw a runtime exception if the primitive's
 * <CODE>valueOf()</CODE> method does not accept it as a valid <CODE>String</CODE> representation of
 * the primitive. <p/> <P>A value written as the row type can be read as the column type. <p/> <PRE>
 * |        | boolean byte short char int long float double String byte[]
 * |---------------------------------------------------------------------- |boolean |    X X |byte
 *  |          X     X         X   X                  X |short   |                X         X   X
 *               X |char    |                     X X |int     |                          X   X
 *             X |long    | X                  X |float   |                                    X X
 *    X |double  |                                          X      X |String  |    X     X X
 * X   X     X     X      X |byte[]  | X |----------------------------------------------------------------------
 * </PRE> <p/> <P>Attempting to read a null value as a primitive type must be treated as calling the
 * primitive's corresponding <code>valueOf(String)</code> conversion method with a null value. Since
 * <code>char</code> does not support a <code>String</code> conversion, attempting to read a null
 * value as a <code>char</code> must throw a <code>NullPointerException</code>.
 *
 * @author Gershon Diner
 * @version 5.1 Copyright: Copyright (c) 2006 Company: GigaSpaces Technologies,Ltd.
 * @see javax.jms.Session#createStreamMessage()
 * @see javax.jms.BytesMessage
 * @see javax.jms.MapMessage
 * @see javax.jms.Message
 * @see javax.jms.ObjectMessage
 * @see javax.jms.TextMessage
 */
public class GSStreamMessageImpl
        extends GSMessageImpl
        implements StreamMessage {
    /** */
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
    private int bytesToRead = -1;

    final static byte _EOF = 2;
    final static byte _BYTES = 3;
    final static byte _STRING = 4;
    final static byte _BOOLEAN = 5;
    final static byte _CHAR = 6;
    final static byte _BYTE = 7;
    final static byte _SHORT = 8;
    final static byte _INT = 9;
    final static byte _LONG = 10;
    final static byte _FLOAT = 11;
    final static byte _DOUBLE = 12;
    final static byte _NULL = 13;

    public GSStreamMessageImpl() throws JMSException {
        this(null);
    }

    /**
     * Instantiates a bright new <code>GSStreamMessageImpl</code>.
     */
    GSStreamMessageImpl(GSSessionImpl session)
            throws JMSException {
        super(session, GSMessageImpl.STREAM);
    }

    /**
     * Reads a <code>boolean</code> from the stream message.
     *
     * @return the <code>boolean</code> value read
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */

    public boolean readBoolean() throws JMSException {
        initializeReading();
        try {
            if (this.dataIn.available() == 0) {
                throw new MessageEOFException("reached end of data");
            }

            this.dataIn.mark(10);
            int type = this.dataIn.read();
            if (type == _BOOLEAN) {
                return this.dataIn.readBoolean();
            }
            if (type == _STRING) {
                return Boolean.valueOf(this.dataIn.readUTF()).booleanValue();
            }
            if (type == _NULL) {
                this.dataIn.reset();
                throw new NullPointerException("Cannot convert _NULL value to boolean.");
            } else {
                this.dataIn.reset();
                throw new MessageFormatException(" not a boolean type");
            }
        } catch (EOFException e) {
            JMSException jmsEx = new MessageEOFException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (IOException e) {
            JMSException jmsEx = new MessageFormatException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }


    /**
     * Reads a <code>byte</code> value from the stream message.
     *
     * @return the next byte from the stream message as a 8-bit <code>byte</code>
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */

    public byte readByte() throws JMSException {
        initializeReading();
        try {
            if (this.dataIn.available() == 0) {
                throw new MessageEOFException("reached end of data");
            }

            this.dataIn.mark(10);
            int type = this.dataIn.read();
            if (type == _BYTE) {
                return this.dataIn.readByte();
            }
            if (type == _STRING) {
                return Byte.valueOf(this.dataIn.readUTF()).byteValue();
            }
            if (type == _NULL) {
                this.dataIn.reset();
                throw new NullPointerException("Cannot convert _NULL value to byte.");
            } else {
                this.dataIn.reset();
                throw new MessageFormatException(" not a byte type");
            }
        } catch (NumberFormatException mfe) {
            try {
                this.dataIn.reset();
            } catch (IOException ioe) {
                JMSException jmsEx = new JMSException("reset failed");
                jmsEx.setLinkedException(ioe);
            }
            throw mfe;

        } catch (EOFException e) {
            JMSException jmsEx = new MessageEOFException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (IOException e) {
            JMSException jmsEx = new MessageFormatException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }


    /**
     * Reads a 16-bit integer from the stream message.
     *
     * @return a 16-bit integer from the stream message
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */

    public short readShort() throws JMSException {
        initializeReading();
        try {
            if (this.dataIn.available() == 0) {
                throw new MessageEOFException("reached end of data");
            }

            this.dataIn.mark(17);
            int type = this.dataIn.read();
            if (type == _SHORT) {
                return this.dataIn.readShort();
            }
            if (type == _BYTE) {
                return this.dataIn.readByte();
            }
            if (type == _STRING) {
                return Short.valueOf(this.dataIn.readUTF()).shortValue();
            }
            if (type == _NULL) {
                this.dataIn.reset();
                throw new NullPointerException("Cannot convert _NULL value to short.");
            } else {
                this.dataIn.reset();
                throw new MessageFormatException(" not a short type");
            }
        } catch (NumberFormatException mfe) {
            try {
                this.dataIn.reset();
            } catch (IOException ioe) {
                JMSException jmsEx = new JMSException("reset failed");
                jmsEx.setLinkedException(ioe);
            }
            throw mfe;

        } catch (EOFException e) {
            JMSException jmsEx = new MessageEOFException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (IOException e) {
            JMSException jmsEx = new MessageFormatException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }

    }


    /**
     * Reads a Unicode character value from the stream message.
     *
     * @return a Unicode character from the stream message
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid
     * @throws MessageNotReadableException if the message is in write-only mode.
     */

    public char readChar() throws JMSException {
        initializeReading();
        try {
            if (this.dataIn.available() == 0) {
                throw new MessageEOFException("reached end of data");
            }

            this.dataIn.mark(17);
            int type = this.dataIn.read();
            if (type == _CHAR) {
                return this.dataIn.readChar();
            }
            if (type == _NULL) {
                this.dataIn.reset();
                throw new NullPointerException("Cannot convert _NULL value to char.");
            } else {
                this.dataIn.reset();
                throw new MessageFormatException(" not a char type");
            }
        } catch (NumberFormatException mfe) {
            try {
                this.dataIn.reset();
            } catch (IOException ioe) {
                JMSException jmsEx = new JMSException("reset failed");
                jmsEx.setLinkedException(ioe);
            }
            throw mfe;

        } catch (EOFException e) {
            JMSException jmsEx = new MessageEOFException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (IOException e) {
            JMSException jmsEx = new MessageFormatException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }


    /**
     * Reads a 32-bit integer from the stream message.
     *
     * @return a 32-bit integer value from the stream message, interpreted as an <code>int</code>
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */

    public int readInt() throws JMSException {
        initializeReading();
        try {
            if (this.dataIn.available() == 0) {
                throw new MessageEOFException("reached end of data");
            }

            this.dataIn.mark(33);
            int type = this.dataIn.read();
            if (type == _INT) {
                return this.dataIn.readInt();
            }
            if (type == _SHORT) {
                return this.dataIn.readShort();
            }
            if (type == _BYTE) {
                return this.dataIn.readByte();
            }
            if (type == _STRING) {
                return Integer.valueOf(this.dataIn.readUTF()).intValue();
            }
            if (type == _NULL) {
                this.dataIn.reset();
                throw new NullPointerException("Cannot convert _NULL value to int.");
            } else {
                this.dataIn.reset();
                throw new MessageFormatException(" not an int type");
            }
        } catch (NumberFormatException mfe) {
            try {
                this.dataIn.reset();
            } catch (IOException ioe) {
                JMSException jmsEx = new JMSException("reset failed");
                jmsEx.setLinkedException(ioe);
            }
            throw mfe;

        } catch (EOFException e) {
            JMSException jmsEx = new MessageEOFException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (IOException e) {
            JMSException jmsEx = new MessageFormatException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }


    /**
     * Reads a 64-bit integer from the stream message.
     *
     * @return a 64-bit integer value from the stream message, interpreted as a <code>long</code>
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */

    public long readLong() throws JMSException {
        initializeReading();
        try {
            if (this.dataIn.available() == 0) {
                throw new MessageEOFException("reached end of data");
            }

            this.dataIn.mark(65);
            int type = this.dataIn.read();
            if (type == _LONG) {
                return this.dataIn.readLong();
            }
            if (type == _INT) {
                return this.dataIn.readInt();
            }
            if (type == _SHORT) {
                return this.dataIn.readShort();
            }
            if (type == _BYTE) {
                return this.dataIn.readByte();
            }
            if (type == _STRING) {
                return Long.valueOf(this.dataIn.readUTF()).longValue();
            }
            if (type == _NULL) {
                this.dataIn.reset();
                throw new NullPointerException("Cannot convert _NULL value to long.");
            } else {
                this.dataIn.reset();
                throw new MessageFormatException(" not a long type");
            }
        } catch (NumberFormatException mfe) {
            try {
                this.dataIn.reset();
            } catch (IOException ioe) {
                JMSException jmsEx = new JMSException("reset failed");
                jmsEx.setLinkedException(ioe);
            }
            throw mfe;

        } catch (EOFException e) {
            JMSException jmsEx = new MessageEOFException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (IOException e) {
            JMSException jmsEx = new MessageFormatException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }


    /**
     * Reads a <code>float</code> from the stream message.
     *
     * @return a <code>float</code> value from the stream message
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */

    public float readFloat() throws JMSException {
        initializeReading();
        try {
            if (this.dataIn.available() == 0) {
                throw new MessageEOFException("reached end of data");
            }

            this.dataIn.mark(33);
            int type = this.dataIn.read();
            if (type == _FLOAT) {
                return this.dataIn.readFloat();
            }
            if (type == _STRING) {
                return Float.valueOf(this.dataIn.readUTF()).floatValue();
            }
            if (type == _NULL) {
                this.dataIn.reset();
                throw new NullPointerException("Cannot convert _NULL value to float.");
            } else {
                this.dataIn.reset();
                throw new MessageFormatException(" not a float type");
            }
        } catch (NumberFormatException mfe) {
            try {
                this.dataIn.reset();
            } catch (IOException ioe) {
                JMSException jmsEx = new JMSException("reset failed");
                jmsEx.setLinkedException(ioe);
            }
            throw mfe;

        } catch (EOFException e) {
            JMSException jmsEx = new MessageEOFException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (IOException e) {
            JMSException jmsEx = new MessageFormatException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }


    /**
     * Reads a <code>double</code> from the stream message.
     *
     * @return a <code>double</code> value from the stream message
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */

    public double readDouble() throws JMSException {
        initializeReading();
        try {
            if (this.dataIn.available() == 0) {
                throw new MessageEOFException("reached end of data");
            }

            this.dataIn.mark(65);
            int type = this.dataIn.read();
            if (type == _DOUBLE) {
                return this.dataIn.readDouble();
            }
            if (type == _FLOAT) {
                return this.dataIn.readFloat();
            }
            if (type == _STRING) {
                return Double.valueOf(this.dataIn.readUTF()).doubleValue();
            }
            if (type == _NULL) {
                this.dataIn.reset();
                throw new NullPointerException("Cannot convert _NULL value to double.");
            } else {
                this.dataIn.reset();
                throw new MessageFormatException(" not a double type");
            }
        } catch (NumberFormatException mfe) {
            try {
                this.dataIn.reset();
            } catch (IOException ioe) {
                JMSException jmsEx = new JMSException("reset failed");
                jmsEx.setLinkedException(ioe);
            }
            throw mfe;

        } catch (EOFException e) {
            JMSException jmsEx = new MessageEOFException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (IOException e) {
            JMSException jmsEx = new MessageFormatException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }


    /**
     * Reads a <CODE>String</CODE> from the stream message.
     *
     * @return a Unicode string from the stream message
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */

    public String readString() throws JMSException {
        initializeReading();
        try {
            if (this.dataIn.available() == 0) {
                throw new MessageEOFException("reached end of data");
            }

            this.dataIn.mark(65);
            int type = this.dataIn.read();
            if (type == _NULL) {
                return null;
            }
            if (type == _STRING) {
                return this.dataIn.readUTF();
            }
            if (type == _LONG) {
                return Long.toString(this.dataIn.readLong());
            }
            if (type == _INT) {
                return Integer.toString(this.dataIn.readInt());
            }
            if (type == _SHORT) {
                return Short.toString(this.dataIn.readShort());
            }
            if (type == _BYTE) {
                return Byte.toString(this.dataIn.readByte());
            }
            if (type == _FLOAT) {
                return Float.toString(this.dataIn.readFloat());
            }
            if (type == _DOUBLE) {
                return Double.toString(this.dataIn.readDouble());
            }
            if (type == _BOOLEAN) {
                return Boolean.toString(this.dataIn.readBoolean());
            }
            if (type == _CHAR) {
                return Character.toString(this.dataIn.readChar());
            } else {
                this.dataIn.reset();
                throw new MessageFormatException(" not a String type");
            }
        } catch (NumberFormatException mfe) {
            try {
                this.dataIn.reset();
            } catch (IOException ioe) {
                JMSException jmsEx = new JMSException("reset failed");
                jmsEx.setLinkedException(ioe);
            }
            throw mfe;

        } catch (EOFException e) {
            JMSException jmsEx = new MessageEOFException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (IOException e) {
            JMSException jmsEx = new MessageFormatException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }


    /**
     * Reads a byte array field from the stream message into the specified <CODE>byte[]</CODE>
     * object (the read buffer). <p/> <P>To read the field value, <CODE>readBytes</CODE> should be
     * successively called until it returns a value less than the length of the read buffer. The
     * value of the bytes in the buffer following the last byte read is undefined. <p/> <P>If
     * <CODE>readBytes</CODE> returns a value equal to the length of the buffer, a subsequent
     * <CODE>readBytes</CODE> call must be made. If there are no more bytes to be read, this call
     * returns -1. <p/> <P>If the byte array field value is null, <CODE>readBytes</CODE> returns -1.
     * <p/> <P>If the byte array field value is empty, <CODE>readBytes</CODE> returns 0. <p/>
     * <P>Once the first <CODE>readBytes</CODE> call on a <CODE>byte[]</CODE> field value has been
     * made, the full value of the field must be read before it is valid to read the next field. An
     * attempt to read the next field before that has been done will throw a
     * <CODE>MessageFormatException</CODE>. <p/> <P>To read the byte field value into a new
     * <CODE>byte[]</CODE> object, use the <CODE>readObject</CODE> method.
     *
     * @param value the buffer into which the data is read
     * @return the total number of bytes read into the buffer, or -1 if there is no more data
     * because the end of the byte field has been reached
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid.
     * @throws MessageNotReadableException if the message is in write-only mode.
     * @see #readObject()
     */

    public int readBytes(byte[] value) throws JMSException {
        initializeReading();
        try {
            if (value == null) {
                throw new NullPointerException();
            }
            if (bytesToRead == 0) {
                bytesToRead = -1;
                return -1;
            } else if (bytesToRead > 0) {
                if (value.length >= bytesToRead) {
                    bytesToRead = 0;
                    return dataIn.read(value, 0, bytesToRead);
                } else {
                    bytesToRead -= value.length;
                    return dataIn.read(value);
                }
            } else {
                if (this.dataIn.available() == 0) {
                    throw new MessageEOFException("reached end of data");
                }
                if (this.dataIn.available() < 1) {
                    throw new MessageFormatException("Not enough data left to read value");
                }
                this.dataIn.mark(value.length + 1);
                int type = this.dataIn.read();
                if (this.dataIn.available() < 1) {
                    return -1;
                }
                if (type != _BYTES) {
                    throw new MessageFormatException("Not a byte array");
                }
                int len = this.dataIn.readInt();

                if (len >= value.length) {
                    bytesToRead = len - value.length;
                    return this.dataIn.read(value);
                } else {
                    bytesToRead = 0;
                    return this.dataIn.read(value, 0, len);
                }
            }
        } catch (EOFException e) {
            JMSException jmsEx = new MessageEOFException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (IOException e) {
            JMSException jmsEx = new MessageFormatException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }


    /**
     * Reads an object from the stream message. <p/> <P>This method can be used to return, in
     * objectified format, an object in the Java programming language ("Java object") that has been
     * written to the stream with the equivalent <CODE>writeObject</CODE> method call, or its
     * equivalent primitive <CODE>write<I>type</I></CODE> method. <p/> <P>Note that byte values are
     * returned as <CODE>byte[]</CODE>, not <CODE>Byte[]</CODE>. <p/> <P>An attempt to call
     * <CODE>readObject</CODE> to read a byte field value into a new <CODE>byte[]</CODE> object
     * before the full value of the byte field has been read will throw a
     * <CODE>MessageFormatException</CODE>.
     *
     * @return a Java object from the stream message, in objectified format (for example, if the
     * object was written as an <CODE>int</CODE>, an <CODE>Integer</CODE> is returned)
     * @throws JMSException                if the JMS provider fails to read the message due to some
     *                                     internal error.
     * @throws MessageEOFException         if unexpected end of message stream has been reached.
     * @throws MessageFormatException      if this type conversion is invalid.
     * @throws MessageNotReadableException if the message is in write-only mode.
     * @see #readBytes(byte[] value)
     */

    public Object readObject() throws JMSException {
        initializeReading();
        try {
            if (this.dataIn.available() == 0) {
                throw new MessageEOFException("reached end of data");
            }

            this.dataIn.mark(65);
            int type = this.dataIn.read();
            if (type == _NULL) {
                return null;
            }
            if (type == _STRING) {
                return this.dataIn.readUTF();
            }
            if (type == _LONG) {
                return Long.valueOf(this.dataIn.readLong());
            }
            if (type == _INT) {
                return Integer.valueOf(this.dataIn.readInt());
            }
            if (type == _SHORT) {
                return Short.valueOf(this.dataIn.readShort());
            }
            if (type == _BYTE) {
                return Byte.valueOf(this.dataIn.readByte());
            }
            if (type == _FLOAT) {
                return Float.valueOf(this.dataIn.readFloat());
            }
            if (type == _DOUBLE) {
                return Double.valueOf(this.dataIn.readDouble());
            }
            if (type == _BOOLEAN) {
                return Boolean.valueOf(this.dataIn.readBoolean());
            }
            if (type == _CHAR) {
                return new Character(this.dataIn.readChar());
            }
            if (type == _BYTES) {
                int len = this.dataIn.readInt();
                byte[] value = new byte[len];
                this.dataIn.read(value);
                return value;
            } else {
                this.dataIn.reset();
                throw new MessageFormatException("unknown type");
            }
        } catch (NumberFormatException mfe) {
            try {
                this.dataIn.reset();
            } catch (IOException ioe) {
                JMSException jmsEx = new JMSException("reset failed");
                jmsEx.setLinkedException(ioe);
            }
            throw mfe;

        } catch (EOFException e) {
            JMSException jmsEx = new MessageEOFException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (IOException e) {
            JMSException jmsEx = new MessageFormatException(e.toString());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }


    /**
     * Writes a <code>boolean</code> to the stream message. The value <code>true</code> is written
     * as the value <code>(byte)1</code>; the value <code>false</code> is written as the value
     * <code>(byte)0</code>.
     *
     * @param value the <code>boolean</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */

    public void writeBoolean(boolean value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(_BOOLEAN);
            this.dataOut.writeBoolean(value);
        } catch (IOException ioe) {
            raise(ioe);
        }
    }


    /**
     * Writes a <code>byte</code> to the stream message.
     *
     * @param value the <code>byte</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */

    public void writeByte(byte value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(_BYTE);
            this.dataOut.writeByte(value);
        } catch (IOException ioe) {
            raise(ioe);
        }
    }


    /**
     * Writes a <code>short</code> to the stream message.
     *
     * @param value the <code>short</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */

    public void writeShort(short value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(_SHORT);
            this.dataOut.writeShort(value);
        } catch (IOException ioe) {
            raise(ioe);
        }
    }


    /**
     * Writes a <code>char</code> to the stream message.
     *
     * @param value the <code>char</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */

    public void writeChar(char value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(_CHAR);
            this.dataOut.writeChar(value);
        } catch (IOException ioe) {
            raise(ioe);
        }
    }


    /**
     * Writes an <code>int</code> to the stream message.
     *
     * @param value the <code>int</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */

    public void writeInt(int value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(_INT);
            this.dataOut.writeInt(value);
        } catch (IOException ioe) {
            raise(ioe);
        }
    }


    /**
     * Writes a <code>long</code> to the stream message.
     *
     * @param value the <code>long</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */

    public void writeLong(long value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(_LONG);
            this.dataOut.writeLong(value);
        } catch (IOException ioe) {
            raise(ioe);
        }
    }


    /**
     * Writes a <code>float</code> to the stream message.
     *
     * @param value the <code>float</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */

    public void writeFloat(float value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(_FLOAT);
            this.dataOut.writeFloat(value);
        } catch (IOException ioe) {
            raise(ioe);
        }
    }


    /**
     * Writes a <code>double</code> to the stream message.
     *
     * @param value the <code>double</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */

    public void writeDouble(double value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(_DOUBLE);
            this.dataOut.writeDouble(value);
        } catch (IOException ioe) {
            raise(ioe);
        }
    }


    /**
     * Writes a <code>String</code> to the stream message.
     *
     * @param value the <code>String</code> value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */

    public void writeString(String value) throws JMSException {
        initializeWriting();
        try {
            if (value == null) {
                this.dataOut.write(_NULL);
            } else {
                this.dataOut.write(_STRING);
                this.dataOut.writeUTF(value);
            }
        } catch (IOException ioe) {
            raise(ioe);
        }
    }


    /**
     * Writes a byte array field to the stream message. <p/> <P>The byte array <code>value</code> is
     * written to the message as a byte array field. Consecutively written byte array fields are
     * treated as two distinct fields when the fields are read.
     *
     * @param value the byte array value to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */

    public void writeBytes(byte[] value) throws JMSException {
        writeBytes(value, 0, value.length);
    }


    /**
     * Writes a portion of a byte array as a byte array field to the stream message. <p/> <P>The a
     * portion of the byte array <code>value</code> is written to the message as a byte array field.
     * Consecutively written byte array fields are treated as two distinct fields when the fields
     * are read.
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
            this.dataOut.write(_BYTES);
            this.dataOut.writeInt(length);
            this.dataOut.write(value, offset, length);
        } catch (IOException ioe) {
            raise(ioe);
        }
    }


    /**
     * Writes an object to the stream message. <p/> <P>This method works only for the objectified
     * primitive object types (<code>Integer</code>, <code>Double</code>,
     * <code>Long</code>&nbsp;...), <code>String</code> objects, and byte arrays.
     *
     * @param value the Java object to be written
     * @throws JMSException                 if the JMS provider fails to write the message due to
     *                                      some internal error.
     * @throws MessageFormatException       if the object is invalid.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */

    public void writeObject(Object value) throws JMSException {
        initializeWriting();
        if (value == null) {
            try {
                this.dataOut.write(_NULL);
            } catch (IOException ioe) {
                raise(ioe);
            }
        } else if (value instanceof String) {
            writeString(value.toString());
        } else if (value instanceof Character) {
            writeChar(((Character) value).charValue());
        } else if (value instanceof Boolean) {
            writeBoolean(((Boolean) value).booleanValue());
        } else if (value instanceof Byte) {
            writeByte(((Byte) value).byteValue());
        } else if (value instanceof Short) {
            writeShort(((Short) value).shortValue());
        } else if (value instanceof Integer) {
            writeInt(((Integer) value).intValue());
        } else if (value instanceof Float) {
            writeFloat(((Float) value).floatValue());
        } else if (value instanceof Double) {
            writeDouble(((Double) value).doubleValue());
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        }
    }


    /**
     * @param bodyAsBytes The bodyAsBytes to set.
     */
    public void setBodyAsBytes(byte[] bodyAsBytes, int offset, int length) {
        //super.setBodyAsBytes(bodyAsBytes,offset,length);
        Body = new ByteArray(bodyAsBytes);
        dataOut = null;
        dataIn = null;
    }

    /**
     * @return Returns the data body
     * @throws IOException if an exception occurs retreiving the data
     */
    public ByteArray getBodyAsBytes() throws IOException {
        if (this.dataOut != null) {
            this.dataOut.flush();
            byte[] data = this.bytesOut.toByteArray();
            //super.setBodyAsBytes(data,0,data.length);
            Body = new ByteArray(data);
            dataOut.close();
            dataOut = null;
        }
        if (Body == null) {
            Body = new ByteArray();
        }
        return (ByteArray) Body;
        //return super.getBodyAsBytes();
    }


    /** Returns the array of bytes body of the message.
     * @return Returns the bodyAsBytes.
     * @throws IOException
     * */
//   public ByteArray getBodyAsBytes() throws IOException
//   {
//	  if (Body == null) 
//	  {
//		  convertBodyToBytes();
//	  }
//	  return Body;
//  }

    /**
     * Convert the message body to data
     *
     * @throws IOException
     */
//   public final void convertBodyToBytes() throws IOException 
//   {
//       if (Body == null) 
//       {
//           ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//           DataOutputStream dataOut = new DataOutputStream(bytesOut);
//           writeBody(dataOut);
//           dataOut.flush();
//           Body = new ByteArray(bytesOut.toByteArray());
//           dataOut.close();
//       }
//   }


    /**
     * Puts the message body in read-only mode and repositions the stream of bytes to the
     * beginning.
     *
     * @throws JMSException if an internal error occurs
     */
    public void reset() throws JMSException {
        setBodyReadOnly(true);
        if (this.dataOut != null) {
            try {
                this.dataOut.flush();
                byte[] data = this.bytesOut.toByteArray();
                //super.setBodyAsBytes(data,0,data.length);
                Body = new ByteArray(data);
                dataOut.close();
            } catch (IOException ioe) {
                JMSException jmsEx = new JMSException("reset failed: " + ioe.toString());
                jmsEx.setLinkedException(ioe);
                throw jmsEx;
            }
        }
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
        ByteArray data;
        if (Body == null) {
            data = new ByteArray();
            Body = data;
        } else {
            data = (ByteArray) Body;
        }
        if (this.dataIn == null && data != null) {
            ByteArrayInputStream bytesIn = new ByteArrayInputStream(data.getBuf(), data.getOffset(), data.getLength());
            this.dataIn = new DataInputStream(bytesIn);
        }
    }

    /**
     * Helper to raise a JMSException when an I/O error occurs
     *
     * @param exception the exception that caused the failure
     */
    private final void raise(IOException exception) throws JMSException {
        JMSException error = new JMSException(exception.toString());
        error.setLinkedException(exception);
        throw error;
    }

    /**
     * Override the super class method to reset the streams, and put the message body in write only
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
        super.clearBody();
        this.dataOut = null;
        this.dataIn = null;
        this.bytesOut = null;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("\n\n\tStreamMessage:");
        sb.append("\n\t\tbytesOut:\t\t");
        sb.append(bytesOut);
        sb.append("\n\t\tdataIn:\t\t\t");
        sb.append(dataIn);
        sb.append("\n\t\tbytesToRead:\t\t");
        sb.append(bytesToRead);
        sb.append("\n\t\tdataOut:\t\t");
        sb.append(dataOut);
        return sb.toString();
    }

}//end of class
