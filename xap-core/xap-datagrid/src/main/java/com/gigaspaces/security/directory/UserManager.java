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
 * The main interface for managing user details.
 *
 * @author Moran Avigdor
 * @see DirectoryManager
 * @since 7.0.1
 */
public interface UserManager {

    /**
     * Indicates that this manager is for read-only purposes, and that any invocation of a
     * destructive operation will throw an {@link AccessDeniedException}.
     *
     * @return <code>true</code> if read-only access is allowed.
     */
    boolean isReadOnly();

    /**
     * Create a new user with the specified <code>userDetails</code>.
     *
     * @param userDetails new user details to store.
     * @throws UserAlreadyExistsException if the user details represent a non-unique user
     * @throws UserDataAccessException    if could not store the specified user details.
     * @throws AccessDeniedException      if read-only access is allowed.
     */
    void createUser(UserDetails userDetails) throws UserAlreadyExistsException, UserDataAccessException, AccessDeniedException;

    /**
     * Delete an existing user represented by the specified <code>username</code>.
     *
     * @param username an existing user name.
     * @throws UserNotFoundException   if the user was not found.
     * @throws UserDataAccessException if could not delete the specified user details.
     * @throws AccessDeniedException   if read-only access is allowed.
     */
    void deleteUser(String username) throws UserNotFoundException, UserDataAccessException, AccessDeniedException;

    /**
     * Retrieve the user details by <code>username</code>.
     *
     * @param username an existing user to retrieve.
     * @return the user details of the specified user.
     * @throws UserNotFoundException   if the user was not found.
     * @throws UserDataAccessException if could not retrieve the specified user details.
     */
    UserDetails getUser(String username) throws UserNotFoundException, UserDataAccessException;

    /**
     * Returns a map containing mapping between username and user-details.
     *
     * @return a mapping between username and user-details.
     * @throws UserDataAccessException if could not retrieve user-details to map.
     */
    Map<String, UserDetails> mapUsers() throws UserDataAccessException;

    /**
     * Updates the user details, excluding the username which must remain the same. In order to
     * update a username, a user must be removed and re-created. A password on the other hand, may
     * require encryption, only if it was modified.
     *
     * @param userDetails the user details to update.
     * @throws UserNotFoundException   if the user was not found.
     * @throws UserDataAccessException if could not update the user details.
     * @throws AccessDeniedException   if read-only access is allowed.
     */
    void updateUser(UserDetails userDetails) throws UserNotFoundException, UserDataAccessException, AccessDeniedException;

    /**
     * Queries for presence of a user by username.
     *
     * @param username the user to check existence for.
     * @return <code>true</code> if the user exists; <code>false</code> otherwise.
     * @throws UserDataAccessException if could not query for the specified user.
     */
    boolean userExists(String username) throws UserDataAccessException;
}
