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

package com.gigaspaces.internal.utils;

import com.j_spaces.kernel.ClassLoaderHelper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;

/**
 * Convenient methods for converting objects to and from bytes.
 *
 * @author Moran Avigdor
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class ByteUtils {

    /**
     * Convert an object into byte array.
     *
     * @param obj an object that implements {@link Serializable}
     * @return a byte array representing this object.
     * @throws java.io.IOException if an I/O error occurs while writing stream header.
     */
    public static byte[] objectToBytes(Object obj) throws java.io.IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.flush();
        oos.close();
        bos.close();
        byte[] data = bos.toByteArray();
        return data;
    }

    /**
     * Convert a byte array into an object.
     *
     * @param bytes a byte array representing the object.
     * @return an object.
     * @throws IOException            if an I/O error occurs while reading stream header.
     * @throws ClassNotFoundException Class of a serialized object cannot be found
     */
    public static Object bytesToObject(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream inStream = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ContextClassLoaderObjectInputStream(inStream);
        Object data = in.readObject();
        in.close();
        return data;
    }

    static class ContextClassLoaderObjectInputStream extends ObjectInputStream {

        public ContextClassLoaderObjectInputStream(InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            try {
                return ClassLoaderHelper.loadClass(desc.getName());
            } catch (ClassNotFoundException e) {
                return super.resolveClass(desc);
            }
        }
    }
}
