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

import com.gigaspaces.security.SecurityManager;

/**
 * An interface for user and role management. The directory manager is created by the call to {@link
 * SecurityManager#createDirectoryManager(UserDetails)}. <p> The directory manager is a standalone
 * component that is used to manage users and roles, outside of the service (server) life-cycle.
 * Management capability is optional if management is done using an external tool. If this is the
 * case, a {@link DirectoryAccessDeniedException} should be thrown in response to the {@link
 * SecurityManager#createDirectoryManager(UserDetails)} call.
 *
 * @author Moran Avigdor
 * @see DirectoryAccessDeniedException
 * @since 7.0.1
 */
public interface DirectoryManager {

    /**
     * A user manager for user-management based on the privileges granted to the directory manager.
     *
     * @return a user manager for managing users
     */
    UserManager getUserManager();

    /**
     * A role manager for role-management based on the privileges granted to the directory manager.
     *
     * @return a role manager for managing roles
     */
    RoleManager getRoleManager();

    /**
     * Close the resource used to store the user and role details. <p> Implementors may choose to
     * flush all gathered details, or remove any excess resources (database connections) that may
     * have been opened upon construction.
     */
    void close();
}
