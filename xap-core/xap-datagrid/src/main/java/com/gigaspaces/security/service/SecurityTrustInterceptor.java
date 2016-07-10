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

/**
 * Used to intercept a trusted security context. A trust is handed out for a user which has already
 * been authenticated and intercepted for a specific operation, and needs further trust to extend
 * its reach for cluster wide operations (e.g. from within an executed task).
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class SecurityTrustInterceptor {
    private static final class TrustedSecurityContext extends SecurityContext {
        private static final long serialVersionUID = 1321427299600425456L;

        /*Externalizable*/
        public TrustedSecurityContext() {
        }

        public TrustedSecurityContext(SecurityContext securityContext) {
            super(securityContext);
        }
    }

    /**
     * Wraps the security context with a trust.
     *
     * @param securityContext security context to trust
     * @return a trusted security context
     */
    SecurityContext trust(SecurityContext securityContext) {
        TrustedSecurityContext trustedContext = new TrustedSecurityContext(securityContext);
        return trustedContext;
    }

    /**
     * Verifies the security context has a valid trust
     *
     * @param securityContext a security context to verify
     * @return <code>true</code> if trusted, <code>false</code> otherwise.
     */
    boolean verifyTrust(SecurityContext securityContext) {
        return (securityContext instanceof TrustedSecurityContext);
    }
}
