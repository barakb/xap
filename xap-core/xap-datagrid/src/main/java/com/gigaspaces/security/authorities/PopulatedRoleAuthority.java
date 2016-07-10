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


package com.gigaspaces.security.authorities;

import com.gigaspaces.security.Authentication;
import com.gigaspaces.security.Authority;
import com.gigaspaces.security.SecurityManager;
import com.gigaspaces.security.directory.UserDetails;

/**
 * A role authority which holds all the authorities this role represents. <p> Commonly, a users'
 * details are stored in one table 'users-table' and its roles are stored in another 'role-table'.
 * The users' authorities of type {@link RoleAuthority} are actually keys (by role-name) to the role
 * details. <p> When authenticating a user, an {@link Authentication} object is returned with all
 * the users' details and authorities. To be ignorant of how details are actually kept, the users'
 * details are populated with a flat representation of all the authorities a role represents. <p>
 * The first option to populate a users' authorities with a role is to just add the authorities a
 * role represents. But, you loose the 'role' representation. If you want to keep it, for example
 * for visibility in tooling, you can return a {@link PopulatedRoleAuthority}. Thus, {@link
 * UserDetails#getAuthorities()} will include user-specific authorities and {@link
 * PopulatedRoleAuthority}. <p> This is only relevant is you are planning to implement your own
 * {@link SecurityManager}. Out default implementation already incorporates this construct.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class PopulatedRoleAuthority extends RoleAuthority {

    private static final long serialVersionUID = 1L;
    private final Authority[] authorities;

    /**
     * Constructs a role authority with all the authorities this role represents.
     *
     * @param role role name
     */
    public PopulatedRoleAuthority(String role, Authority[] authorities) {
        super(role);
        this.authorities = authorities;
    }

    /**
     * Returns the granted authorities granted to the role. Cannot return <code>null</code>.
     *
     * @return the authorities (never <code>null</code>)
     */
    Authority[] getAuthorities() {
        return authorities;
    }

}
