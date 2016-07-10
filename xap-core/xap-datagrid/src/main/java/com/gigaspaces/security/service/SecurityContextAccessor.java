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

//Internal Doc
package com.gigaspaces.security.service;

import com.gigaspaces.security.session.SessionDetails;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.filters.ISpaceFilter;

/**
 * Accessor method for setting Security Context attributes which their setter method should not be
 * directly exposed.
 *
 * @author Moran Avigdor
 * @since 7.0.2
 */
@com.gigaspaces.api.InternalApi
public class SecurityContextAccessor {

    /**
     * Fill security context to be passed into the {@link ISpaceFilter} as part of the {@link
     * SpaceContext}. This will be used to provide filtering capabilities based on security
     * restriction requirements.
     *
     * @param securityContext The security context to fill with the provided session details.
     * @param sessionDetails  The session details stored in the server (space).
     */
    public static void fillSecurityContext(SecurityContext securityContext, SessionDetails sessionDetails) {
        securityContext.applySessionDetails(sessionDetails);
    }
}
