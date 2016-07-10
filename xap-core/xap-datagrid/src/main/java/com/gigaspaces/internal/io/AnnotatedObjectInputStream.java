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


package com.gigaspaces.internal.io;

import com.j_spaces.kernel.ClassLoaderHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.rmi.server.RMIClassLoader;

/**
 * An extension of <code>ObjectInputStream</code> that implements the dynamic class loading
 * semantics of RMI argument and result unmarshalling (using {@link RMIClassLoader}).  A
 * <code>AnnotatedObjectInputStream</code> is intended to read data written by a corresponding
 * {@link AnnotatedObjectOutputStream}.
 *
 * <p><code>AnnotatedObjectInputStream</code> implements the input side of the dynamic class loading
 * semantics by overriding {@link ObjectInputStream#resolveClass resolveClass} and {@link
 * ObjectInputStream#resolveProxyClass resolveProxyClass} to resolve class descriptors in the stream
 * using {@link ClassLoading#loadClass ClassLoading.loadClass} and {@link
 * ClassLoading#loadProxyClass ClassLoading.loadProxyClass} (which, in turn, use {@link
 * RMIClassLoader#loadClass(String, String, ClassLoader) RMIClassLoader.loadClass} and {@link
 * RMIClassLoader#loadProxyClass(String, String[], ClassLoader) RMIClassLoader.loadProxyClass}),
 * optionally with CodeBase annotation strings written by a <code>AnnotatedObjectOutputStream</code>.
 *
 * <p><code>AnnotatedObjectInputStream</code> reads class annotations from its own stream; a
 * subclass can override the {@link #readAnnotation readAnnotation} method to read the class
 * annotations from a different location.
 *
 * <p>A <code>AnnotatedObjectInputStream</code> is not guaranteed to be safe for concurrent use by
 * multiple threads.
 *
 * @author Igor Goldenberg
 * @author anna
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class AnnotatedObjectInputStream
        extends ObjectInputStream {

    /**
     * Creates a new <code>AnnotatedObjectInputStream</code> that reads marshalled data from the
     * specified underlying <code>InputStream</code>.
     *
     * <p>This constructor passes <code>in</code> to the superclass constructor that has an
     * <code>InputStream</code> parameter.
     *
     * <p><code>defaultLoader</code> will be passed as the <code>defaultLoader</code> argument to
     * {@link ClassLoading#loadClass ClassLoading.loadClass} and {@link ClassLoading#loadProxyClass
     * ClassLoading.loadProxyClass} whenever those methods are invoked by {@link #resolveClass
     * resolveClass} and {@link #resolveProxyClass resolveProxyClass}.
     *
     * <p>If <code>verifyCodebaseIntegrity</code> is <code>true</code>, then the created stream will
     * verify that all codebase annotation URLs that are used to load classes resolved by the stream
     * provide content integrity, and whenever {@link Security#verifyCodebaseIntegrity
     * Security.verifyCodebaseIntegrity} is invoked to enforce that verification,
     * <code>verifierLoader</code> will be passed as the <code>loader</code> argument.  See {@link
     * ClassLoading#loadClass ClassLoading.loadClass} and {@link ClassLoading#loadProxyClass
     * ClassLoading.loadProxyClass} for details of how codebase integrity verification is
     * performed.
     *
     * <p><code>context</code> will be used as the return value of the created stream's {@link
     * #getObjectStreamContext getObjectStreamContext} method.
     *
     * @param in            the input stream to read marshalled data from
     * @param defaultLoader the class loader value (possibly <code>null</code>) to pass as the
     *                      <code>defaultLoader</code> argument to <code>ClassLoading</code>
     *                      methods
     * @param context       the collection of context information objects to be returned by this
     *                      stream's {@link #getObjectStreamContext getObjectStreamContext} method
     * @throws IOException          if the superclass's constructor throws an <code>IOException</code>
     * @throws SecurityException    if the superclass's constructor throws a <code>SecurityException</code>
     * @throws NullPointerException if <code>in</code> or <code>context</code> is <code>null</code>
     **/
    public AnnotatedObjectInputStream(InputStream in) throws IOException {
        super(in);

    }

    /**
     * Resolves the appropriate {@link Class} object for the stream class descriptor
     * <code>classDesc</code>.
     *
     * <p><code>AnnotatedObjectInputStream</code> implements this method as follows:
     *
     * <p>Invokes this stream's {@link #readAnnotation readAnnotation} method to read the annotation
     * string value (possibly <code>null</code>) for the class descriptor.  If
     * <code>readAnnotation</code> throws an exception, then this method throws that exception.
     * Otherwise, a codebase value is chosen as follows: if the {@link #useCodebaseAnnotations
     * useCodebaseAnnotations} method has been invoked on this stream, then the codebase value
     * chosen is the value that was returned by <code>readAnnotation</code>; otherwise, the codebase
     * value chosen is <code>null</code>.
     *
     * <p>This method then invokes {@link ClassLoading#loadClass ClassLoading.loadClass} with the
     * chosen codebase value as the first argument, the name of the class described by
     * <code>classDesc</code> as the second argument, and the <code>defaultLoader</code>,
     * <code>verifyCodebaseIntegrity</code>, and <code>verifierLoader</code> values that were passed
     * to this stream's constructor as the third, fourth, and fifth arguments. If
     * <code>ClassLoading.loadClass</code> throws a <code>ClassNotFoundException</code> and the name
     * of the class described by <code>classDesc</code> equals the Java(TM) programming language
     * keyword for a primitive type or <code>void</code>, then this method returns the
     * <code>Class</code> corresponding to that primitive type or <code>void</code> ({@link
     * Integer#TYPE} for <code>int</code>, {@link Void#TYPE} for <code>void</code>, and so forth).
     * Otherwise, if <code>ClassLoading.loadClass</code> throws an exception, this method throws
     * that exception, and if it returns normally, this method returns the <code>Class</code>
     * returned by <code>ClassLoading.loadClass</code>.
     *
     * @param classDesc the stream class descriptor to resolve
     * @return the resolved class
     * @throws IOException            if <code>readAnnotation</code> throws an <code>IOException</code>,
     *                                or if <code>ClassLoading.loadClass</code> throws a
     *                                <code>MalformedURLException</code>
     * @throws ClassNotFoundException if <code>readAnnotation</code> or <code>ClassLoading.loadClass</code>
     *                                throws a <code>ClassNotFoundException</code>
     * @throws NullPointerException   if <code>classDesc</code> is <code>null</code>
     **/
    protected Class resolveClass(ObjectStreamClass classDesc, boolean readAnnotation, String codebase)
            throws IOException, ClassNotFoundException {
        if (readAnnotation) {
            codebase = readAnnotation(classDesc);
        }
        String name = classDesc.getName();
        try {
            return ClassLoaderHelper.loadClass(name);
        } catch (ClassNotFoundException e) {
            return RMIClassLoader.loadClass(codebase, name, Thread.currentThread().getContextClassLoader());
        }
    }

    @Override
    protected Class resolveClass(ObjectStreamClass classDesc)
            throws IOException, ClassNotFoundException {
        return resolveClass(classDesc, true, null);
    }

    /**
     * Reads and returns a class annotation string value (possibly <code>null</code>) that was
     * written by a corresponding <code>AnnotatedObjectOutputStream</code> implementation.
     *
     * <p><code>AnnotatedObjectInputStream</code> implements this method to just read the annotation
     * value from this stream using {@link ObjectInputStream#readUnshared readUnshared}, and if
     * <code>readUnshared</code> returns a non-<code>null</code> value that is not a
     * <code>String</code>, an {@link InvalidObjectException} is thrown.
     *
     * <p>A subclass can override this method to read the annotation from a different location.
     *
     * @return the class annotation string value read (possibly <code>null</code>)
     * @throws IOException            if an I/O exception occurs reading the annotation
     * @throws ClassNotFoundException if a <code>ClassNotFoundException</code> occurs reading the
     *                                annotation
     **/
    protected String readAnnotation(ObjectStreamClass desc) throws IOException,
            ClassNotFoundException {
        try {
            return (String) readUnshared();
        } catch (ClassCastException e) {
            InvalidObjectException ioe = new InvalidObjectException("Annotation is not String or null");
            ioe.initCause(e);
            throw ioe;
        }
    }


}