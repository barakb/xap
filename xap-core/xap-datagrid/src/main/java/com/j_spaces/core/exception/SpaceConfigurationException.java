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


package com.j_spaces.core.exception;

import java.rmi.RemoteException;

/**
 * This exception is thrown in case of space configuration issues.
 *
 * @version 4.5
 */


public class SpaceConfigurationException extends RemoteException {

    private static final long serialVersionUID = 3257567287128045107L;

    /**
     * Constructs a <code>SpaceConfigurationException</code>.
     */
    public SpaceConfigurationException() {
        super();
    }

    /**
     * Constructs a <code>SpaceConfigurationException</code> with the specified detail message.
     *
     * @param str - the detail message
     */
    public SpaceConfigurationException(String str) {
        super(str);
    }

    /**
     * Constructs a <code>SpaceConfigurationException</code> with the specified detail message and
     * cause.
     *
     * @param str   the detail message
     * @param cause the cause
     */
    public SpaceConfigurationException(String str, Throwable cause) {
        super(str, cause);
    }

}
