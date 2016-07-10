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

import com.gigaspaces.internal.io.GSByteArrayOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.lang.reflect.Proxy;


/**
 * Serialization helper utility class
 *
 * @author Gershon Diner
 * @version 1.0
 * @since 5.1
 */
public class SerializationHelper {

    private static final GSByteArrayOutputStream outStream = new GSByteArrayOutputStream(65535);

    static final private ClassLoader GIGASPACES_JMS_CLASSLOADER = SerializationHelper.class.getClassLoader();

    /**
     * Serialize an Object to a ByteArray
     *
     * @return a byte[] array
     */
    public static byte[] writeObject(Object object) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(buffer);
        out.writeObject(object);
        out.close();
        return buffer.toByteArray();
    }

    /**
     * Read an Object from a byte array
     *
     * @return answer
     */
    public static Object readObject(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream buffer = new ByteArrayInputStream(data);
        ObjectInputStreamExt in = new ObjectInputStreamExt(buffer);
        Object answer = in.readObject();
        in.close();
        return answer;
    }

    /**
     * read an object from an InputStream
     *
     * @return result
     */
    public static Object readObject(InputStream in) throws IOException, ClassNotFoundException {
        ObjectInputStreamExt objIn = new ObjectInputStreamExt(in);
        Object result = objIn.readObject();
        return result;

    }

    /**
     * Creates an object from a byte buffer.
     **/
    public static Object objectFromByteBuffer(byte[] buffer)
            throws Exception {
        if (buffer == null)
            return null;

        ByteArrayInputStream inStream = new ByteArrayInputStream(buffer);
        ObjectInputStream in = new ObjectInputStreamExt(inStream);
        Object retval = in.readObject();
        in.close();

        return retval;
    }

    /**
     * Serializes an object into a byte buffer. The object has to implement interface Serializable
     * or Externalizable.
     **/
    public static byte[] objectToByteBuffer(Object obj)
            throws Exception {
        byte[] result = null;
        synchronized (outStream) {
            outStream.reset();
            ObjectOutputStream out = new ObjectOutputStream(outStream);
            out.writeObject(obj);
            out.flush();
            result = outStream.toByteArray();
            out.close();
        }

        return result;
    }


    /**
     * A deep copy makes a distinct copy of each of the object's fields, recursing through the
     * entire graph of other objects referenced by the object being copied. Deep clone by serialize
     * and deserialize the object and return the deserialized version. A deep copy/clone, assuming
     * everything in the tree is serializable.
     *
     * NOTE: This method is very expensive!, don't use this method if you need performance.
     *
     * @param obj the object to clone, the object and object context must implement
     *            java.io.Serializable.
     * @return the copied object include all object references.
     * @throws IllegalArgumentException Failed to perform deep clone. The object of the context
     *                                  object is not implements java.io.Serializable.
     **/
    public static Object deepClone(Object obj) {
        try {
            byte[] bArray = objectToByteBuffer(obj);
            return objectFromByteBuffer(bArray);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to perform deep clone on [" + obj + "] object. Check that the all object context are implements java.io.Serializable.", ex);
        }
    }

    /**
     * Write an Object to an OutputStream
     */
    public static void writeObject(OutputStream out, Object obj) throws IOException {
        ObjectOutputStream objOut = new ObjectOutputStream(out);
        objOut.writeObject(obj);
        objOut.flush();
    }

    static public class ObjectInputStreamExt extends ObjectInputStream {

        public ObjectInputStreamExt(InputStream in) throws IOException {
            super(in);
        }

        protected Class resolveClass(ObjectStreamClass classDesc) throws IOException, ClassNotFoundException {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            return load(classDesc.getName(), cl);
        }

        protected Class resolveProxyClass(String[] interfaces) throws IOException, ClassNotFoundException {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Class[] cinterfaces = new Class[interfaces.length];
            for (int i = 0; i < interfaces.length; i++)
                cinterfaces[i] = load(interfaces[i], cl);

            try {
                return Proxy.getProxyClass(cinterfaces[0].getClassLoader(), cinterfaces);
            } catch (IllegalArgumentException e) {
                throw new ClassNotFoundException(null, e);
            }
        }

        private Class load(String className, ClassLoader cl) throws ClassNotFoundException {
            try {
                return ClassLoading.loadClass(className, cl);
            } catch (ClassNotFoundException e) {
                return ClassLoading.loadClass(className, GIGASPACES_JMS_CLASSLOADER);
            }
        }

    }

}
