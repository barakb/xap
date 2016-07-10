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
 * Defines an Authority for managing of users and roles. <p> The <code>SystemAuthority</code>s'
 * {@link Authority#getAuthority()} String representation format:
 * <pre>
 * <code>SystemPrivilege privilege-value</code>
 *
 * Where:
 * privilege-value = MANAGE_ROLES | MANAGE_USERS
 *
 * The privileges represent the following system operations:
 * MANAGE_ROLES - managing of roles
 * MANAGE_USERS - managing of users
 * </pre>
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class SystemAuthority implements InternalAuthority {

    /**
     * Defines monitoring privileges
     */
    public enum SystemPrivilege implements Privilege {
        /**
         * managing of roles
         */
        MANAGE_ROLES,
        /**
         * managing of users
         */
        MANAGE_USERS;

        @Override
        public String toString() {
            switch (this) {
                case MANAGE_ROLES:
                    return "Manage Roles";
                case MANAGE_USERS:
                    return "Manage Users";
                default:
                    return super.toString();
            }
        }
    }

    private static final long serialVersionUID = 1L;
    private final SystemPrivilege systemPrivilege;

    public SystemAuthority(SystemPrivilege systemPrivilege) {
        this.systemPrivilege = systemPrivilege;
    }

    public static SystemAuthority valueOf(String authority) {
        String[] split = authority.split(Constants.DELIM);
        if (split.length < 2) {
            throw new IllegalArgumentException("Illegal Authority format: " + authority);
        }

        if (!SystemPrivilege.class.getSimpleName().equals(split[Constants.PRIVILEGE_NAME_POS])) {
            throw new IllegalArgumentException("Illegal Authority name in: " + authority);
        }

        SystemPrivilege systemPrivilege = SystemPrivilege.valueOf(split[Constants.PRIVILEGE_VAL_POS]);
        return new SystemAuthority(systemPrivilege);
    }

    /*
     * @see com.gigaspaces.security.Authority#getAuthority()
     */
    public String getAuthority() {
        return systemPrivilege.getClass().getSimpleName() + Constants.DELIM + systemPrivilege.name();
    }

    /*
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return getAuthority();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((getAuthority() == null) ? 0 : getAuthority().hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SystemAuthority other = (SystemAuthority) obj;
        if (getAuthority() == null) {
            if (other.getAuthority() != null)
                return false;
        } else if (!getAuthority().equals(other.getAuthority()))
            return false;
        return true;
    }


    /*
     * @see com.gigaspaces.security.authorities.InternalAuthority#getMappingKey()
     */
    public Privilege getPrivilege() {
        return systemPrivilege;
    }
}
