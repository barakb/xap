/*
 * Copyright 2005 Sun Microsystems, Inc.
 * Copyright 2005 GigaSpaces, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jini.rio.resources.resource;

/**
 * Exception to indicate that there are no available resources that became available during the
 * specified wait time.
 */
@com.gigaspaces.api.InternalApi
public class ResourceUnavailableException extends Exception {
    static final long serialVersionUID = 1L;

    /**
     * Constructs a ResourceUnavailableException with no detail message. A detail message is a
     * String that describes this particular exception.
     */
    public ResourceUnavailableException() {
        super();
    }

    /**
     * Constructs a ResourceUnavailableException with the specified detail message. A detail message
     * is a String that describes this particular exception.
     *
     * @param s the detail message
     */
    public ResourceUnavailableException(String s) {
        super(s);
    }
}
