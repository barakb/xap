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

package com.j_spaces.core;

/**
 * A SpaceSecurityException is thrown if a security filter failes during init/add process or during
 * process routine, it causes the operation to abort, or if init/add failed- abort space creation.
 *
 * @author Yechiel Fefer
 * @version 1.0
 **/
@Deprecated
@com.gigaspaces.api.InternalApi
public class SpaceSecurityException extends SecurityException {
    private static final long serialVersionUID = 1L;

    public SpaceSecurityException() {
        super();
    }

    public SpaceSecurityException(String s) {
        super(s);
    }

    public SpaceSecurityException(String s, Throwable cause) {
        super(s);
        initCause(cause);
    }

    @Override
    public String toString() {
        return "SpaceSecurityException() " + super.toString();
    }
}
