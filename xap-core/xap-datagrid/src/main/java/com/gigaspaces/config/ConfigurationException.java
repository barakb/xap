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

package com.gigaspaces.config;

/**
 * Thrown if a problem occurs when obtaining configuration information. If the problem results from
 * an exception being thrown while processing the configuration information, that original exception
 * can be accessed by calling {@link #getCause getCause}. Note that any instance of
 * <code>Error</code> thrown while processing the configuration information is propagated to the
 * caller; it is not wrapped in a <code>ConfigurationException</code>.
 *
 * @author Igor Goldenberg
 * @since 5.2
 **/
@com.gigaspaces.api.InternalApi
public class ConfigurationException extends Exception {
    private static final long serialVersionUID = 1L;

    public ConfigurationException(String msg) {
        super(msg);
    }

    public ConfigurationException(String msg, Exception ex) {
        super(msg, ex);
    }

    /**
     * Returns a short description of this exception. The result includes the name of the actual
     * class of this object; the result of calling the {@link #getMessage getMessage} method for
     * this object, if the result is not <code>null</code>; and the result of calling
     * <code>toString</code> on the causing exception, if that exception is not <code>null</code>.
     **/
    public String toString() {
        String s = super.toString();
        Throwable cause = getCause();
        return (cause == null) ? s : (s + "; caused by:\n\t" + cause);
    }
}
