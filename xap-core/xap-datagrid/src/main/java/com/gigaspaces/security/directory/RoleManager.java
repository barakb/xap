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

import com.gigaspaces.security.AccessDeniedException;

import java.util.Map;

/**
 * The main interface for managing role details.
 *
 * @author Moran Avigdor
 * @see DirectoryManager
 * @since 7.0.1
 */
public interface RoleManager {

    /**
     * Indicates that this manager is for read-only purposes, and that any invocation of a
     * destructive operation will throw an {@link AccessDeniedException}.
     *
     * @return <code>true</code> if read-only access is allowed.
     */
    boolean isReadOnly();

    /**
     * Create a new role with the specified <code>roleDetails</code>.
     *
     * @param roleDetails new role details to store.
     * @throws RoleAlreadyExistsException if the role details represent a non-unique role
     * @throws RoleDataAccessException    if could not store the specified role details.
     * @throws AccessDeniedException      if read-only access is allowed.
     */
    void createRole(RoleDetails roleDetails) throws RoleAlreadyExistsException, RoleDataAccessException, AccessDeniedException;

    /**
     * Delete an existing role represented by the specified <code>role</code>.
     *
     * @param role an existing role name.
     * @throws RoleNotFoundException   if the role was not found.
     * @throws RoleDataAccessException if could not delete the specified role details.
     * @throws AccessDeniedException   if read-only access is allowed.
     */
    void deleteRole(String role) throws RoleNotFoundException, RoleDataAccessException, AccessDeniedException;

    /**
     * Retrieve the role details by <code>role</code>.
     *
     * @param role an existing role to retrieve.
     * @return the role details of the specified role.
     * @throws RoleNotFoundException   if the role was not found.
     * @throws RoleDataAccessException if could not retrieve the specified role details.
     */
    RoleDetails getRole(String role) throws RoleNotFoundException, RoleDataAccessException;

    /**
     * Returns a map containing mapping between role and role-details.
     *
     * @return a mapping between role name and role-details.
     * @throws RoleDataAccessException if could not retrieve role-details to map.
     */
    Map<String, RoleDetails> mapRoles() throws RoleDataAccessException;

    /**
     * Updates the role details, excluding the role which must remain the same. In order to update a
     * role, a role must be removed and re-created.
     *
     * @param roleDetails the role details to update.
     * @throws RoleNotFoundException   if the role was not found.
     * @throws RoleDataAccessException if could not update the role details.
     * @throws AccessDeniedException   if read-only access is allowed.
     */
    void updateRole(RoleDetails roleDetails) throws RoleNotFoundException, RoleDataAccessException, AccessDeniedException;

    /**
     * Queries for presence of a role by role.
     *
     * @param role the role to check existence for.
     * @return <code>true</code> if the role exists; <code>false</code> otherwise.
     * @throws RoleDataAccessException if could not query for the specified role.
     */
    boolean roleExists(String role) throws RoleDataAccessException;

}
