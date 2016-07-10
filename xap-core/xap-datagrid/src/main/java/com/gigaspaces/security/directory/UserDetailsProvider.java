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

import java.util.Properties;

/**
 * A {@link UserDetails} provider used by the UI/CLI or other tools to create a custom {@link
 * UserDetails} instance. <p> The {@link DefaultUserDetailsProvider} returns the default {@link
 * UserDetails} implementation (see {@link User} which is based on username and password.
 *
 * @since 7.1.4, 8.0.1
 * @deprecated Use {@link CredentialsProvider} instead.
 */
@Deprecated
public interface UserDetailsProvider {

    /**
     * Create a {@link UserDetails} instance using the provided properties.
     *
     * @param properties properties to extract key value pairs in order to create a {@link
     *                   UserDetails} instance.
     * @return a new user details instance
     * @throws Exception if failed to create the {@link UserDetails} instance.
     */
    UserDetails create(Properties properties) throws Exception;
}