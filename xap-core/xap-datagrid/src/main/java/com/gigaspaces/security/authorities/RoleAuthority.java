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

import com.gigaspaces.security.Authority;

/**
 * Defines an association of a role by-name. <p> The <code>RoleAuthority</code>s' {@link
 * Authority#getAuthority()} String representation format:
 * <pre>
 * <code>RolePrivilege role</code>
 *
 * Where:
 * role = role-name
 * </pre>
 *
 * @author Moran Avigdor
 * @since 7.0
 */

public class RoleAuthority implements Authority {

    /**
     * Empty placeholder for Role privilege (part of authority format)
     */
    public enum RolePrivilege implements Privilege {
    }

    private static final long serialVersionUID = 1L;
    private final String role;

    /**
     * A role authority with a specified role name.
     *
     * @param role The role name.
     */
    public RoleAuthority(String role) {
        this.role = role;
    }


    /**
     * Parses the {@link #getAuthority()} string representation of an Authority.
     *
     * @param authority The role authority to be parsed.
     * @return an instance of the authority represented by the authority string.
     */
    public static RoleAuthority valueOf(String authority) {
        if (authority == null) {
            throw new IllegalArgumentException("Illegal Authority format: null");
        }

        String[] split = authority.split(Constants.DELIM);

        if (split.length < 2) {
            throw new IllegalArgumentException("Illegal Authority format: " + authority);
        }

        if (!RolePrivilege.class.getSimpleName().equals(split[Constants.PRIVILEGE_NAME_POS])) {
            throw new IllegalArgumentException("Illegal Privilege name in: " + authority);
        }

        String role = split[Constants.PRIVILEGE_VAL_POS];
        return new RoleAuthority(role);
    }

    /*
     * @see com.gigaspaces.security.Authority#getAuthority()
     */
    public String getAuthority() {
        return RolePrivilege.class.getSimpleName() + Constants.DELIM + role;
    }

    /*
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return getAuthority();
    }

    /*
     * @see com.gigaspaces.security.authorities.InternalAuthority#getRole()
     */
    public String getRole() {
        return role;
    }
}
