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

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.rmi.server.RMIClassLoader;


/**
 * An extension of <code>ObjectOutputStream</code> that implements the dynamic class loading
 * semantics of RMI argument and result marshalling (using {@link RMIClassLoader}).  A
 * <code>AnnotatedObjectOutputStream</code> writes data that is intended to be written by a
 * corresponding {@link AnnotatedObjectInputStream}.
 *
 * <p><code>AnnotatedObjectOutputStream</code> implements the output side of the dynamic class
 * loading semantics by overriding {@link ObjectOutputStream#annotateClass annotateClass} and {@link
 * ObjectOutputStream#annotateProxyClass annotateProxyClass} to annotate class descriptors in the
 * stream with codebase strings obtained using {@link RMIClassLoader#getClassAnnotation
 * RMIClassLoader.getClassAnnotation}.
 *
 * <p><code>AnnotatedObjectOutputStream</code> writes class annotations to its own stream; a
 * subclass may override the {@link #writeAnnotation writeAnnotation} method to write the class
 * annotations to a different location.
 *
 * <p><code>AnnotatedObjectOutputStream</code> does not modify the stream protocol version of its
 * instances' superclass state (see {@link ObjectOutputStream#useProtocolVersion
 * ObjectOutputStream.useProtocolVersion}).
 *
 * @author Igor Goldenberg
 * @author anna
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class AnnotatedObjectOutputStream
        extends ObjectOutputStream {

    private static final boolean support_code_base = Boolean.getBoolean("com.gs.transport_protocol.lrmi.support-codebase");

    /**
     * Creates a new <code>AnnotatedObjectOutputStream</code> that writes marshaled data to the
     * specified underlying <code>OutputStream</code>.
     *
     * <p>This constructor passes <code>out</code> to the superclass constructor that has an
     * <code>OutputStream</code> parameter.
     *
     * <p><code>context</code> will be used as the return value of the created stream's {@link
     * #getObjectStreamContext getObjectStreamContext} method.
     *
     * @param out     the output stream to write marshaled data to
     * @param context the collection of context information objects to be returned by this stream's
     *                {@link #getObjectStreamContext getObjectStreamContext} method
     * @throws IOException          if the superclass's constructor throws an <code>IOException</code>
     * @throws SecurityException    if the superclass's constructor throws a <code>SecurityException</code>
     * @throws NullPointerException if <code>out</code> or <code>context</code> is
     *                              <code>null</code>
     **/
    public AnnotatedObjectOutputStream(OutputStream out)
            throws IOException {
        super(out);

    }


    /**
     * Annotates the stream descriptor for the class <code>cl</code>.
     *
     * <p><code>AnnotatedObjectOutputStream</code> implements this method as follows:
     *
     * <p>This method invokes this stream's {@link #writeAnnotation writeAnnotation} method with the
     * given cl.
     *
     * @param cl the class to annotate
     * @throws IOException          if <code>writeAnnotation</code> throws an <code>IOException</code>
     * @throws NullPointerException if <code>cl</code> is <code>null</code>
     **/
    @Override
    protected void annotateClass(Class cl) throws IOException {
        writeAnnotation(cl);
    }

    /**
     * Annotates the stream descriptor for the proxy class <code>cl</code>.
     *
     * <p><code>AnnotatedObjectOutputStream</code> implements this method as follows:
     *
     * <p>This method invokes this stream's {@link #writeAnnotation writeAnnotation} method with the
     * given cl.
     *
     * @param cl the proxy class to annotate
     * @throws IOException          if <code>writeAnnotation</code> throws an <code>IOException</code>
     * @throws NullPointerException if <code>cl</code> is <code>null</code>
     **/
    @Override
    protected void annotateProxyClass(Class cl) throws IOException {
        writeAnnotation(cl);
    }


    /**
     * First checks that this class wasn't written already, if it does do nothing.
     *
     * <p>Else invokes {@link RMIClassLoader#getClassAnnotation RMIClassLoader.getClassAnnotation}
     * with <code>cl</code> to get the appropriate class annotation string value (possibly
     * <code>null</code>) and then writes a class annotation string value (possibly
     * <code>null</code>) to be read by a corresponding <code>AnnotatedObjectInputStream</code>
     * implementation.
     *
     * <p><code>AnnotatedObjectOutputStream</code> implements this method to just write the
     * annotation value to this stream using {@link ObjectOutputStream#writeUnshared
     * writeUnshared}.
     *
     * <p>A subclass can override this method to write the annotation to a different location.
     *
     * @param annotation the class annotation string value (possibly <code>null</code>) to write
     * @throws IOException if I/O exception occurs writing the annotation
     **/
    protected void writeAnnotation(Class cl) throws IOException {

        String annotation = support_code_base ? RMIClassLoader.getClassAnnotation(cl) : null;

        writeUnshared(annotation);
    }


}