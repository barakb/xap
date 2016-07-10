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

import com.gigaspaces.security.directory.DirectoryAccessDeniedException;
import com.gigaspaces.security.directory.DirectoryManager;
import com.gigaspaces.security.directory.UserDetails;

import java.util.Properties;

/**
 * Interface for authentication and creating of the directory manager. The
 * <code>SecurityManager</code> can be obtained by the {@link SecurityFactory}. The properties
 * represent the service specific properties that will be used to initialize the
 * <code>SecurityManager</code> instance.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
public interface SecurityManager {
    /**
     * The property key identifying the security manager in a properties file/object
     */
    public static final String SECURITY_MANAGER_CLASS_PROPERTY_KEY = "com.gs.security.security-manager.class";

    /**
     * Initializes this security manager instance with implementation specific properties, and any
     * resource creation/access needed.
     *
     * @param properties properties to use configure this instance.
     */
    void init(Properties properties) throws SecurityException;

    /**
     * Attempts to authenticate the passed user represented by {@link UserDetails}, returning a
     * fully populated <code>UserDetails</code> object (including granted authorities) if
     * successful.
     *
     * @param userDetails the user request object
     * @return a fully authenticated object including authorities
     * @throws AuthenticationException if authentication fails
     */
    Authentication authenticate(UserDetails userDetails) throws AuthenticationException;

    /**
     * Attempts to authenticate the passed user represented by {@link UserDetails}, granting access
     * only for users that are allowed to manage the directory. <p> Creates a directory manager for
     * managing of users and roles, granting access only for users that are allowed to manage, but
     * may be restrictive (to read-only) based on their privileges. <p> If the directory is to be
     * managed by an external tool, implementors may wish to throw {@link
     * DirectoryAccessDeniedException} to deny access to the API (e.g. from UI).
     *
     * @param userDetails the user request object
     * @return a directory manager instance
     * @throws AuthenticationException        if authentication fails for the specified user
     * @throws AccessDeniedException          if not granted sufficient access required to manage
     *                                        role details
     * @throws DirectoryAccessDeniedException if the directory should not be managed by API.
     */
    DirectoryManager createDirectoryManager(UserDetails userDetails) throws AuthenticationException,
            AccessDeniedException;

    /**
     * Closes any excess resource kept by the security manager; e.g. connection to a data-source.
     */
    void close();
}
