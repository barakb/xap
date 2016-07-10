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
 * @(#)CacheTimeoutException.java 1.0   24/12/2003  13:14:03
 */

package com.j_spaces.core.client;

/**
 * This Exception thrown when the entry is locked under another transaction and operation timeout
 * expired. Thrown by: get(), remove(), put(), putAll() operations.
 *
 * @author Igor Goldenberg
 * @version 3.2
 * @see com.j_spaces.map.IMap
 **/

public class CacheTimeoutException extends CacheException {
    private static final long serialVersionUID = 8769386174075509454L;

    private String key;

    /**
     * @param key the entry's key the caused the <code>CacheTimeoutException</code>
     */
    public CacheTimeoutException(Object key) {
        super("Entry '" + key + "' is locked under another transaction. Operation timeout expired.");
        this.key = String.valueOf(key);
    }

    /**
     * Returns the entry key that caused the error.
     *
     * @return the entry's key
     */
    public String getKey() {
        return key;
    }
}