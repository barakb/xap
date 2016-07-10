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

package org.openspaces.core;

import com.gigaspaces.security.SecurityException;

import org.springframework.dao.PermissionDeniedDataAccessException;

/**
 * Thrown for a failed operation on a secured service, due to either failed authentication or access
 * denial. Encapsulates any exception that is a subclass of {@link SecurityException}.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
public class SecurityAccessException extends PermissionDeniedDataAccessException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a security exception with a message and cause.
     */
    public SecurityAccessException(Throwable cause) {
        super(cause.getMessage(), cause);
    }
}
