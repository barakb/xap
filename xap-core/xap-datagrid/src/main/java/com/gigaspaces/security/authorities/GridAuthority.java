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
 * Defines an Authority for managing grid services, with the specified privilege. <p> The
 * <code>GridAuthority</code>s' {@link Authority#getAuthority()} String representation format:
 * <pre>
 * <code>GridPrivilege privilege-value</code>
 *
 * Where:
 * privilege-value = PROVISION_PU | MANAGE_PU | MANAGE_GRID
 *
 * The privileges represent the following grid operations:
 * PROVISION_PU - deploy, un-deploy
 * MANAGE_PU    - scale up/down, relocate, restart PU instance, destroy PU instance
 * MANAGE_GRID  - start, terminate, restart of GSC/GSM/LUS
 * </pre>
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class GridAuthority implements InternalAuthority {

    /**
     * Defines managing services privileges
     */
    public enum GridPrivilege implements Privilege {
        /**
         * deploy, un-deploy
         */
        PROVISION_PU,
        /**
         * scale up/down, relocate, restart PU instance, destroy PU instance
         */
        MANAGE_PU,
        /**
         * start, terminate, restart of GSC/GSM/LUS
         */
        MANAGE_GRID;

        @Override
        public String toString() {
            switch (this) {
                case PROVISION_PU:
                    return "Provision PU";
                case MANAGE_PU:
                    return "Manage PU";
                case MANAGE_GRID:
                    return "Manage Grid";
                default:
                    return super.toString();
            }
        }
    }

    private static final long serialVersionUID = 1L;
    private final GridPrivilege gridPrivilege;

    public GridAuthority(GridPrivilege gridPrivilege) {
        this.gridPrivilege = gridPrivilege;
    }

    public static GridAuthority valueOf(String authority) {
        if (authority == null) {
            throw new IllegalArgumentException("Illegal Authority format: null");
        }

        String[] split = authority.split(Constants.DELIM);
        if (split.length < 2) {
            throw new IllegalArgumentException("Illegal Authority format: " + authority);
        }

        if (!GridPrivilege.class.getSimpleName().equals(split[Constants.PRIVILEGE_NAME_POS])) {
            throw new IllegalArgumentException("Illegal Privilege name in: " + authority);
        }
        GridPrivilege gridPrivilege = GridPrivilege.valueOf(split[Constants.PRIVILEGE_VAL_POS]);
        return new GridAuthority(gridPrivilege);
    }

    /*
     * @see com.gigaspaces.security.Authority#getAuthority()
     */
    public String getAuthority() {
        return gridPrivilege.getClass().getSimpleName() + Constants.DELIM + gridPrivilege.name();
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
        GridAuthority other = (GridAuthority) obj;
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
        return gridPrivilege;
    }
}
