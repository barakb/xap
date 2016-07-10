/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package net.jini.space;

/**
 * This exception denotes a problem with the local implementation of the <code>JavaSpace</code>
 * interface.  The <code>detail</code> field will give a description that can be reported to the
 * space developer (and may be documented in that space's external documentation).
 *
 * @author Sun Microsystems, Inc.
 * @see JavaSpace
 */
@com.gigaspaces.api.InternalApi
public class InternalSpaceException extends RuntimeException {
    static final long serialVersionUID = -4167507833172939849L;


    /**
     * Create an exception, forwarding a string to the superclass constructor.
     *
     * @param str a detail message
     */
    public InternalSpaceException(String str) {
        super(str);

    }

    /**
     * @param cause
     */
    public InternalSpaceException(Throwable cause) {
        super(cause);

    }

    /**
     * Create an exception, forwarding a string and exception to the superclass constructor.
     *
     * @param str a detail message
     * @param ex  a nested exception
     */
    public InternalSpaceException(String str, Throwable ex) {
        super(str, ex);
    }

}
