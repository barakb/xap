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

import com.gigaspaces.security.SecurityException;
import com.gigaspaces.security.service.SecurityContext;

@com.gigaspaces.api.InternalApi
public class SpaceContextHelper {

    /**
     * Helper method to extract a non-null security context from a space context.
     *
     * @return security context
     * @throws SecurityException if space context is null or if security context is null.
     */
    public static SecurityContext getSecurityContext(SpaceContext spaceContext) {
        if (spaceContext == null) {
            throw new SecurityException("Invalid space context");
        }
        SecurityContext securityContext = spaceContext.getSecurityContext();
        if (securityContext == null) {
            throw new SecurityException("Invalid security context");
        }
        return securityContext;
    }
}
