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


package com.gigaspaces.client.protective;

/**
 * Thrown when protective mode prevents from wrong usage of the system {@link ProtectiveMode}
 *
 * @author eitany
 * @since 9.1
 */

public class ProtectiveModeException
        extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ProtectiveModeException(String message) {
        super(message);
    }

    public ProtectiveModeException(String message, Throwable cause) {
        super(message, cause);
    }

}
