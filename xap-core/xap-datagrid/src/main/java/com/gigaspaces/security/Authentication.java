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


package com.gigaspaces.security;

import com.gigaspaces.security.authorities.GrantedAuthorities;
import com.gigaspaces.security.directory.UserDetails;

import java.io.Serializable;

/**
 * Represents an authenticated request, returned on successful call to {@link
 * SecurityManager#authenticate(UserDetails)}. <p> An <tt>Authentication</tt> encapsulates the
 * {@link UserDetails} and the corresponding populated authorities.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
public final class Authentication implements Serializable {

    private static final long serialVersionUID = 1L;
    private final UserDetails userDetails;
    private final GrantedAuthorities grantedAuthorities;

    /**
     * Create an authentication for an authenticated {@link UserDetails}.
     *
     * @param userDetails authenticated user details.
     */
    public Authentication(UserDetails userDetails) {
        this.userDetails = userDetails;
        this.grantedAuthorities = new GrantedAuthorities(userDetails.getAuthorities());
    }

    /**
     * The underlying user details used to authenticate
     *
     * @return the authenticated user details.
     */
    public UserDetails getUserDetails() {
        return userDetails;
    }

    /**
     * The underlying user details authorities, parsed into a queriable form.
     *
     * @return parsed authorities.
     */
    public GrantedAuthorities getGrantedAuthorities() {
        return grantedAuthorities;
    }
}
