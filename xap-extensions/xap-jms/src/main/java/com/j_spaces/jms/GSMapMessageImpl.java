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
 * Created on 22/04/2004
 *
 * @author		 	 Gershon Diner
 * Title:        		 The GigaSpaces Platform
 * Copyright:    	 Copyright (c) GigaSpaces Team 2004
 * Company:      	 GigaSpaces Technologies Ltd.
 * @version 	 	 4.0
 */
package com.j_spaces.jms;

import com.j_spaces.jms.utils.ConversionHelper;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;

/**
 * A MapMassage implementation for the GigaSpaces JMS API wrapping.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class GSMapMessageImpl
        extends GSMessageImpl
        implements MapMessage {

    private static final long serialVersionUID = 1L;


    /**
     * Instantiates a new <code>MapMessage</code>.
     *
     * @throws JMSException if falied to create the message
     */
    public GSMapMessageImpl() throws JMSException {
        super();
    }


    /**
     * Instantiates a new <code>MapMessage</code>. It's important to note that map should contain
     * only values that are valid according to the JMS specification.
     *
     * @param session the session of the message
     * @param map     the map content of the message
     * @throws JMSException if falied to create the message
     */
    public GSMapMessageImpl(GSSessionImpl session, HashMap<String, Object> map)
            throws JMSException {
        super(session, MAP);
        Body = map;
    }

    /**
     * Instantiates a new <code>MapMessage</code>. It's important to note that map should contain
     * only values that are valid according to the JMS specification.
     *
     * @param session the session of the message
     * @throws JMSException if falied to create the message
     */
    public GSMapMessageImpl(GSSessionImpl session)
            throws JMSException {
        this(session, null);
    }

    /**
     * Sets the object's map. It's important to note that map should contain only values that are
     * valid according to the JMS specification.
     *
     * @param map the map
     */
    public void setMap(HashMap<String, Object> map) {
        Body = map;
    }

    /**
     * Returns the object's map.
     *
     * @return the object's map
     */
    public HashMap<String, Object> getMap() {
        return (HashMap<String, Object>) Body;
    }


    /**
     * @see MapMessage#getBoolean(String)
     */
    public final boolean getBoolean(String name) throws JMSException, MessageFormatException {
        HashMap body = (HashMap) Body;
        return ConversionHelper.getBoolean(body.get(name));
    }

    /**
     * @see MapMessage#getByte(String)
     */
    public final byte getByte(String name) throws JMSException, MessageFormatException {
        HashMap body = (HashMap) Body;
        return ConversionHelper.getByte(body.get(name));
    }

    /**
     * @see MapMessage#getShort(String)
     */
    public final short getShort(String name) throws JMSException, MessageFormatException {
        HashMap body = (HashMap) Body;
        return ConversionHelper.getShort(body.get(name));
    }

    /**
     * @see MapMessage#getChar(String)
     */
    public final char getChar(String name) throws JMSException, MessageFormatException {
        HashMap body = (HashMap) Body;
        return ConversionHelper.getChar(body.get(name));
    }

    /**
     * @see MapMessage#getInt(String)
     */
    public final int getInt(String name) throws JMSException, MessageFormatException {
        HashMap body = (HashMap) Body;
        return ConversionHelper.getInt(body.get(name));
    }

    /**
     * @see MapMessage#getLong(String)
     */
    public final long getLong(String name) throws JMSException, MessageFormatException {
        HashMap body = (HashMap) Body;
        return ConversionHelper.getLong(body.get(name));
    }

    /**
     * @see MapMessage#getFloat(String)
     */
    public final float getFloat(String name) throws JMSException, MessageFormatException {
        HashMap body = (HashMap) Body;
        return ConversionHelper.getFloat(body.get(name));
    }

    /**
     * @see MapMessage#getDouble(String)
     */
    public final double getDouble(String name) throws JMSException, MessageFormatException {
        HashMap body = (HashMap) Body;
        return ConversionHelper.getDouble(body.get(name));
    }

    /**
     * @see MapMessage#getString(String)
     */
    public final String getString(String name) throws JMSException, MessageFormatException {
        HashMap body = (HashMap) Body;
        return ConversionHelper.getString(body.get(name));
    }

    /**
     * @see MapMessage#getBytes(String)
     */
    public final byte[] getBytes(String name) throws JMSException, MessageFormatException {
        HashMap body = (HashMap) Body;
        return ConversionHelper.getBytes(body.get(name));
    }

    /**
     * Return the Java object value with the given name <p> Note that this method can be used to
     * return in objectified format, an object that had been stored in the Map with the equivalent
     * <code>setObject</code> method call, or it's equivalent primitive set<type> method.
     *
     * @param name the name of the Java object
     * @return a copy of the Java object value with the given name, in objectified format (e.g. if
     * it set as an int, then an Integer is returned). Note that byte values are returned as byte[],
     * not Byte[]. If there is no item by this name, a null value is returned.
     * @throws JMSException if JMS fails to read the message due to some internal JMS error
     */
    public final Object getObject(String name) throws JMSException {
        Object result = null;
        HashMap body = (HashMap) Body;
        Object value = body.get(name);
        if (value != null) {
            if (value instanceof Boolean) {
                result = value;
            } else if (value instanceof Byte) {
                result = value;
            } else if (value instanceof Short) {
                result = value;
            } else if (value instanceof Character) {
                result = value;
            } else if (value instanceof Integer) {
                result = value;
            } else if (value instanceof Long) {
                result = value;
            } else if (value instanceof Float) {
                result = value;
            } else if (value instanceof Double) {
                result = value;
            } else if (value instanceof String) {
                result = value;
            } else if (value instanceof byte[]) {
                result = getBytes(name);
            } else {
                throw new MessageFormatException("MapMessage contains an unsupported Java primitive type:  "
                        + value.getClass().getName());
            }
        }
        return result;
    }

    /**
     * Return an Enumeration of all the Map message's names.
     *
     * @return an enumeration of all the names in this Map message.
     * @see MapMessage#getMapNames()
     */
    public Enumeration getMapNames() throws JMSException {
        HashMap body = (HashMap) Body;
        return Collections.enumeration(body.keySet());
    }

    /**
     * Set a boolean value with the given name, into the Map
     *
     * @param name  the name of the boolean
     * @param value the boolean value to set in the Map
     * @throws JMSException if the message is in read-only mode
     */
    public final void setBoolean(String name, boolean value)
            throws JMSException {
        checkPropertiesReadOnly();
        HashMap body = (HashMap) Body;
        body.put(name, Boolean.valueOf(value));
    }

    /**
     * Set a byte value with the given name, into the Map
     *
     * @param name  the name of the byte
     * @param value the byte value to set in the Map
     * @throws JMSException if the message is in read-only mode
     */
    public final void setByte(String name, byte value)
            throws JMSException {
        checkPropertiesReadOnly();
        HashMap body = (HashMap) Body;
        body.put(name, Byte.valueOf(value));
    }

    /**
     * Set a short value with the given name, into the Map
     *
     * @param name  the name of the short
     * @param value the short value to set in the Map
     * @throws JMSException if the message is in read-only mode
     */
    public final void setShort(String name, short value)
            throws JMSException {
        checkPropertiesReadOnly();
        HashMap body = (HashMap) Body;
        body.put(name, Short.valueOf(value));
    }

    /**
     * Set a Unicode character value with the given name, into the Map
     *
     * @param name  the name of the Unicode character
     * @param value the Unicode character value to set in the Map
     * @throws JMSException if the message is in read-only mode
     */
    public final void setChar(String name, char value)
            throws JMSException {
        checkPropertiesReadOnly();
        HashMap body = (HashMap) Body;
        body.put(name, Character.valueOf(value));
    }

    /**
     * Set an integer value with the given name, into the Map
     *
     * @param name  the name of the integer
     * @param value the integer value to set in the Map
     * @throws JMSException if the message is in read-only mode
     */
    public final void setInt(String name, int value)
            throws JMSException {
        checkPropertiesReadOnly();
        HashMap body = (HashMap) Body;
        body.put(name, Integer.valueOf(value));
    }

    /**
     * Set a long value with the given name, into the Map
     *
     * @param name  the name of the long
     * @param value the long value to set in the Map
     * @throws JMSException if the message is in read-only mode
     */
    public final void setLong(String name, long value)
            throws JMSException {
        checkPropertiesReadOnly();
        HashMap body = (HashMap) Body;
        body.put(name, Long.valueOf(value));
    }

    /**
     * Set a float value with the given name, into the Map
     *
     * @param name  the name of the float
     * @param value the float value to set in the Map
     * @throws JMSException if the message is in read-only mode
     */
    public final void setFloat(String name, float value)
            throws JMSException {
        checkPropertiesReadOnly();
        HashMap body = (HashMap) Body;
        body.put(name, Float.valueOf(value));
    }

    /**
     * Set a double value with the given name, into the Map
     *
     * @param name  the name of the double
     * @param value the double value to set in the Map
     * @throws JMSException if the message is in read-only mode
     */
    public final void setDouble(String name, double value)
            throws JMSException {
        checkPropertiesReadOnly();
        HashMap body = (HashMap) Body;
        body.put(name, Double.valueOf(value));
    }

    /**
     * Set a String value with the given name, into the Map
     *
     * @param name  the name of the String
     * @param value the String value to set in the Map
     * @throws JMSException if the message is in read-only mode
     */
    public final void setString(String name, String value)
            throws JMSException {
        checkPropertiesReadOnly();
        HashMap body = (HashMap) Body;
        body.put(name, value);
    }

    /**
     * Set a byte array value with the given name, into the Map
     *
     * @param name  the name of the byte array
     * @param value the byte array value to set in the Map. The array is copied so the value for
     *              name will not be altered by future modifications.
     * @throws JMSException if the message is in read-only mode
     */
    public final void setBytes(String name, byte[] value)
            throws JMSException {
        checkPropertiesReadOnly();
        byte[] bytes = null;
        if (value != null) {
            bytes = new byte[value.length];
            System.arraycopy(value, 0, bytes, 0, bytes.length);
        }
        HashMap body = (HashMap) Body;
        body.put(name, bytes);
    }

    /**
     * Set a portion of the byte array value with the given name, into the Map
     *
     * @param name   the name of the byte array
     * @param value  the byte array value to set in the Map.
     * @param offset the initial offset within the byte array.
     * @param length the number of bytes to use.
     * @throws JMSException if the message is in read-only mode
     */
    public final void setBytes(String name, byte[] value,
                               int offset, int length)
            throws JMSException {
        checkPropertiesReadOnly();
        byte[] bytes = null;
        if (value != null) {
            bytes = new byte[length];
            System.arraycopy(value, offset, bytes, 0, length);
        }
        HashMap body = (HashMap) Body;
        body.put(name, bytes);
    }

    /**
     * Set a Java object value with the given name, into the Map <p> Note that this method only
     * works for the objectified primitive object types (Integer, Double, Long ...), String's and
     * byte arrays.
     *
     * @param name  the name of the Java object
     * @param value the Java object value to set in the Map
     * @throws MessageFormatException if object is invalid
     * @throws JMSException           if message in read-only mode.
     */
    public final void setObject(String name, Object value)
            throws JMSException {
        checkPropertiesReadOnly();
        if (value == null) {
            HashMap body = (HashMap) Body;
            body.put(name, null);
        } else if (value instanceof Boolean) {
            setBoolean(name, ((Boolean) value).booleanValue());
        } else if (value instanceof Byte) {
            setByte(name, ((Byte) value).byteValue());
        } else if (value instanceof Short) {
            setShort(name, ((Short) value).shortValue());
        } else if (value instanceof Character) {
            setChar(name, ((Character) value).charValue());
        } else if (value instanceof Integer) {
            setInt(name, ((Integer) value).intValue());
        } else if (value instanceof Long) {
            setLong(name, ((Long) value).longValue());
        } else if (value instanceof Float) {
            setFloat(name, ((Float) value).floatValue());
        } else if (value instanceof Double) {
            setDouble(name, ((Double) value).doubleValue());
        } else if (value instanceof String) {
            setString(name, (String) value);
        } else if (value instanceof byte[]) {
            setBytes(name, (byte[]) value);
        } else {
            throw new MessageFormatException(
                    "MapMessage does not support objects of type=" +
                            value.getClass().getName());
        }
    }

    /**
     * Check if an item exists in this MapMessage
     *
     * @param name the name of the item to test
     * @return true if the item exists
     */
    public boolean itemExists(String name) throws JMSException {
        HashMap body = (HashMap) Body;
        return body.containsKey(name);
    }

    /**
     * API method.
     *
     * @throws JMSException Actually never thrown.
     */
    public void clearBody() throws JMSException {
        super.clearBody();
        Body = new HashMap<String, Object>();
    }


    /**
     * Creates a copy of this message.
     *
     * @return the copy of this message.
     */
    GSMessageImpl duplicate() throws JMSException {
        GSMapMessageImpl dup = new GSMapMessageImpl();
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
        HashMap<String, Object> body = (HashMap<String, Object>) Body;
        HashMap<String, Object> cloned = new HashMap<String, Object>(body.size());
        cloned.putAll(body);
        return cloned;
    }
}//end of class
