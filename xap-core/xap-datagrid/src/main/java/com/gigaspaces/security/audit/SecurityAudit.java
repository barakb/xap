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

package com.gigaspaces.security.audit;

import com.gigaspaces.security.AuthenticationException;
import com.gigaspaces.security.AuthenticationToken;
import com.gigaspaces.security.authorities.Privilege;
import com.gigaspaces.security.service.SecurityContext;
import com.gigaspaces.security.session.SessionDetails;

/**
 * Security auditing interface
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
public interface SecurityAudit {

    /**
     * Callback upon authentication failure.
     *
     * @param securityContext         Security context used to authenticate.
     * @param authenticationException reason for authentication failure.
     */
    void authenticationFailed(SecurityContext securityContext, AuthenticationException authenticationException);

    /**
     * Callback upon authentication success.
     *
     * @param in  Security context used to authenticate
     * @param out Security context returned upon successful authentication
     */
    void authenticationSuccessful(SecurityContext in, SecurityContext out);

    /**
     * @param token Security context holding the authentication token.
     */
    void authenticationInvalid(AuthenticationToken token);

    /**
     * Callback on access denied for a Space operation.
     *
     * @param securityContext Security context holding the authentication token
     * @param sessionDetails  details of this session
     * @param privilege       required privilege
     * @param className       The class name on which to operate
     */
    void accessDenied(SecurityContext securityContext, SessionDetails sessionDetails, Privilege privilege, String className);

    /**
     * Callback on access granted for a Space operation.
     *
     * @param securityContext Security context holding the authentication token
     * @param privilege       The privilege granted
     * @param className       The class name on which to operate
     */
    void accessGranted(SecurityContext securityContext, SessionDetails sessionDetails, Privilege privilege, String className);
}
