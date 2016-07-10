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

import com.j_spaces.kernel.JSpaceUtilities;

import java.util.Properties;

/**
 * A default {@link UserDetails} provider which returns the default {@link User} with the specified
 * username and password property values.
 *
 * @since 7.1.4, 8.0.1
 * @deprecated Use {@link DefaultCredentialsProvider} instead.
 */
@Deprecated

public class DefaultUserDetailsProvider implements UserDetailsProvider {

    public UserDetails create(Properties properties) {

        String userName = properties.getProperty(CredentialsProvider.USERNAME_PROPERTY);
        if (JSpaceUtilities.isEmpty(userName)) {
            throw new IllegalArgumentException("Username is required - either null or empty");
        }

        String password = properties.getProperty(CredentialsProvider.PASSWORD_PROPERTY);
        if (JSpaceUtilities.isEmpty(password)) {
            throw new IllegalArgumentException("Password is required - either null or empty");
        }

        return new User(userName, password);
    }
}