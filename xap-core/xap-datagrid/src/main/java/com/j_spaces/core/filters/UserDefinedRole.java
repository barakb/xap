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

package com.j_spaces.core.filters;

import com.j_spaces.core.SecurityContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * @author Michael Konnikov
 * @version 4.1
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class UserDefinedRole implements Serializable, Cloneable {
    static final long serialVersionUID = 1L;

    public String name;
    /**
     * If not null, contains the previous role name, indicates that role has modified
     */
    public String previousName;
    /**
     * Can be inherited form "W" or/and "R" system roles
     */
    // TODO change List to HashSet
    public List<String> inheritRoles;
    /**
     * Map of restricted entries contains: key - entry type, value - list of attributes
     */
    public Map<String, List<GenericPrincipal.Attribute>> restrictedEntries;

    public UserDefinedRole(String name, List<String> inheritRoles, Map<String, List<GenericPrincipal.Attribute>> restrictedEntries) {
        this.name = name;
        this.inheritRoles = inheritRoles;
        this.restrictedEntries = restrictedEntries;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object roleName) {
        return name.equals(roleName);
    }

    /////////////////////////////////////////////////////////////////////////////////
    public static String parseRolesList(List<String> roles) {
        if (roles == null || roles.isEmpty())
            return "";

        StringBuilder sb = new StringBuilder();
        for (Iterator<String> it = roles.iterator(); it.hasNext(); ) {
            sb.append(it.next());
            if (it.hasNext())
                sb.append(", ");
        }
        return sb.toString();
    }

    /**
     * Return list of inherit system roles for user defined role
     */
    public static List<String> parseRolesString(String roles) {
        List<String> result = new ArrayList<String>();
        StringTokenizer st = new StringTokenizer(roles, ",");
        for (; st.hasMoreTokens(); ) {
            String role = st.nextToken().trim();
            if (!result.contains(role) &&
                    (role.equalsIgnoreCase(String.valueOf(SecurityContext.PERMISSION_WRITE)) ||
                            role.equalsIgnoreCase(String.valueOf(SecurityContext.PERMISSION_READ)) ||
                            role.equalsIgnoreCase(String.valueOf(SecurityContext.PERMISSION_EXECUTE))
                    ))
                result.add(role.toUpperCase());
        }
        return result;
    }

    /**
     * Creates and returns a copy of this object.
     *
     * @see java.lang.Object#clone()
     */
    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException ex) {
            // this shouldn't happen, since we are not Cloneable
            throw new InternalError();
        }
    }

}
