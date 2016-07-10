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


package com.gigaspaces.security.directory;

import com.gigaspaces.security.Authority;
import com.gigaspaces.security.authorities.RoleAuthority;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A helper class for managing the directory.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class DirectoryManagerUtils {

    /**
     * Get all users that are assigned to the specified role.
     *
     * @param userManager A user manager to query.
     * @param role        The role to query.
     * @return A list of users with the specified role.
     */
    public static List<UserDetails> getUsersWithRole(UserManager userManager, String role) {
        Map<String, UserDetails> map = userManager.mapUsers();
        List<UserDetails> usersWithRole = new ArrayList<UserDetails>(map.size());
        for (UserDetails user : map.values()) {
            Authority[] authorities = user.getAuthorities();
            for (Authority authority : authorities) {
                if (authority instanceof RoleAuthority) {
                    if (((RoleAuthority) authority).getRole().equals(role)) {
                        usersWithRole.add(user);
                        break;
                    }
                }
            }
        }
        return usersWithRole;
    }

    /**
     * Query the {@link UserManager} for all the roles assigned to the specified user.
     *
     * @param userManager A user manager to query.
     * @param username    The user to query.
     * @return A list of roles for the specified user.
     */
    public static List<String> getUserRoles(UserManager userManager, String username) {
        Map<String, UserDetails> map = userManager.mapUsers();
        List<String> userRoles = new ArrayList<String>();
        for (UserDetails user : map.values()) {
            if (!user.getUsername().equals(username)) {
                continue;
            }
            Authority[] authorities = user.getAuthorities();
            for (Authority authority : authorities) {
                if (authority instanceof RoleAuthority) {
                    String role = ((RoleAuthority) authority).getRole();
                    userRoles.add(role);
                }
            }
        }
        return userRoles;
    }


    /**
     * Extract all roles assigned to the specified user.
     *
     * @param user The user to extract roles from.
     * @return A list of roles for the specified user.
     */
    public static List<String> getUserRoles(UserDetails user) {
        List<String> userRoles = new ArrayList<String>();
        Authority[] authorities = user.getAuthorities();
        for (Authority authority : authorities) {
            if (authority instanceof RoleAuthority) {
                String role = ((RoleAuthority) authority).getRole();
                userRoles.add(role);
            }
        }
        return userRoles;
    }
}
