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
 * Created on 17/05/2004
 *
 * @author		  Gershon Diner
 * Title:        	  The GigaSpaces Platform
 * Copyright:     Copyright (c) GigaSpaces Team 2004
 * Company:      GigaSpaces Technologies Ltd.
 * @version 	 4.0
 */
package com.j_spaces.jms;

import com.j_spaces.jms.utils.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;

/**
 * Implements the <code>javax.jms.ObjectMessage</code> interface for the GigaSpaces JMS API. An
 * <CODE>ObjectMessage</CODE> object is used to send a message that contains a serializable object
 * in the Java programming language ("Java object"). It inherits from the <CODE>Message</CODE>
 * interface and adds a body containing a single reference to an object. Only
 * <CODE>Serializable</CODE> Java objects can be used.
 *
 * <P>If a collection of Java objects must be sent, one of the <CODE>Collection</CODE> classes
 * provided since JDK 1.2 can be used.
 *
 * <P>When a client receives an <CODE>ObjectMessage</CODE>, it is in read-only mode. If a client
 * attempts to write to the message at this point, a <CODE>MessageNotWriteableException</CODE> is
 * thrown. If <CODE>clearBody</CODE> is called, the message can now be both read from and written
 * to.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 * @see javax.jms.Session#createObjectMessage()
 * @see javax.jms.Session#createObjectMessage(Serializable)
 * @see javax.jms.Message
 */
public class GSObjectMessageImpl
        extends GSMessageImpl
        implements ObjectMessage {

    private static final long serialVersionUID = 1L;


    /**
     * Instantiates a new <code>GSObjectMessageImpl</code>.
     *
     * @throws JMSException if falied to create the message
     */
    public GSObjectMessageImpl() throws JMSException {
        super();
    }


    /**
     * Instantiates a new <code>GSObjectMessageImpl</code>.
     *
     * @param session the session of the message
     * @param objBody the object content of the message
     * @throws JMSException if falied to create the message
     */
    public GSObjectMessageImpl(GSSessionImpl session, Serializable objBody)
            throws JMSException {
        super(session, GSMessageImpl.OBJECT);
        setObject(objBody);
    }


    /**
     * Instantiates a new <code>GSObjectMessageImpl</code>.
     *
     * @param session the session of the message
     * @throws JMSException if falied to create the message
     */
    public GSObjectMessageImpl(GSSessionImpl session) throws JMSException {
        this(session, null);
    }


    /**
     * Set the serializable object containing this message's data. It is important to note that an
     * <code>ObjectMessage</code> contains a snapshot of the object at the time
     * <code>setObject()</code> is called - subsequent modifications of the object will have no
     * affect on the <code>ObjectMessage</code> body.
     *
     * @param obj the message's data
     * @throws MessageFormatException       if object serialization fails
     * @throws MessageNotWriteableException if the message is read-only
     */
    public final void setObject(Serializable obj) throws JMSException {
        checkBodyReadOnly();
        Body = cloneObject(obj);
    }


    /**
     * Get the serializable object containing this message's data. The default value is null.
     *
     * @return the serializable object containing this message's data
     * @throws MessageFormatException if object deserialization fails
     */
    public final Serializable getObject() throws MessageFormatException {
        Object clone = cloneBody();
        if (clone != null) {
            return (Serializable) clone;
        }
        return null;
    }


    /**
     * Used serialize the message body to an output stream
     */
    public void writeBody(DataOutput dataOut) throws IOException {
        SerializationHelper.writeObject((OutputStream) dataOut, Body);
    }


    /**
     * Used to help build the body from an input stream
     */
    public void readBody(DataInput dataIn) throws IOException {
        try {
            Body = SerializationHelper.readObject((InputStream) dataIn);
        } catch (ClassNotFoundException ex) {
            throw new IOException(ex.toString());
        }
    }

    /**
     * Creates a copy of this message.
     *
     * @return the copy of this message.
     */
    GSMessageImpl duplicate() throws JMSException {
        GSObjectMessageImpl dup = new GSObjectMessageImpl();
        copyTo(dup);
        return dup;
    }

    /**
     * Returns a clone of the body.
     *
     * @return a clone of the body.
     */
    protected Object cloneBody() {
        return cloneObject(Body);
    }

}
