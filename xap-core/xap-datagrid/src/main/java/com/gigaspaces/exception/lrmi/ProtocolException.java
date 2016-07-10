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

package com.gigaspaces.exception.lrmi;


import com.gigaspaces.lrmi.ConnPoolInvocationHandler;
import com.gigaspaces.lrmi.nio.CPeer;

/**
 * A <code>ProtocolException</code> is thrown as a result of a remote method invocation while
 * unmarshalling the arguments to get request/reply packet.
 *
 * A <code>ProtocolException</code> instance contains the original <code>java.lang.Throwable</code>
 * that occurred as its cause.
 *
 * @author Igor Goldenberg
 * @see CPeer
 * @see ConnPoolInvocationHandler
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class ProtocolException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a <code>ProtocolException</code> with the specified detail message and nested
     * exception.
     *
     * @param s  the detail message.
     * @param ex the nested exception, can't be null.
     * @throws IllegalArgumentException throws if the cause object is null.
     **/
    public ProtocolException(String s, Throwable ex) {
        super(s, ex);

        if (ex == null)
            throw new IllegalArgumentException("The cause object can't be null.");
    }
}
