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
package com.gigaspaces.security.session;

import com.gigaspaces.security.Authentication;
import com.gigaspaces.security.audit.AuditDetails;
import com.gigaspaces.security.service.SecurityContext;

/**
 * A holder for session details at server-side.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class SessionDetails {
    private final Authentication authentication;
    private final AuditDetails auditDetails;

    public SessionDetails(Authentication authentication, SecurityContext securityContext) {
        this.authentication = authentication;
        this.auditDetails = securityContext.getAuditDetails();
    }

    /**
     * @return the authentication
     */
    public Authentication getAuthentication() {
        return authentication;
    }

    /**
     * @return the auditDetails
     */
    public AuditDetails getAuditDetails() {
        return auditDetails;
    }
}
