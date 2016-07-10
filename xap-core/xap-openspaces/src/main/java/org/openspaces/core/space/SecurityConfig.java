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


package org.openspaces.core.space;

import com.gigaspaces.security.directory.CredentialsProvider;
import com.gigaspaces.security.directory.CredentialsProviderHelper;
import com.gigaspaces.security.directory.DefaultCredentialsProvider;

import org.springframework.util.StringUtils;

import java.util.Properties;

/**
 * A configuration object allowing to configure security context (username, password) when working
 * with the Space.
 *
 * @author kimchy
 */
public class SecurityConfig {

    private String username;
    private String password;
    private CredentialsProvider credentialsProvider;

    public SecurityConfig() {
    }

    public SecurityConfig(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public SecurityConfig(CredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }

    /**
     * Returns the username to connect to the Space with.
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets the username to connect to the Space with.
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Returns the password to connect to the Space with.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password to connect to the Space with.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    public CredentialsProvider getCredentialsProvider() {
        if (credentialsProvider != null)
            return credentialsProvider;
        return isFilled() ? new DefaultCredentialsProvider(username, password) : null;
    }

    public boolean isFilled() {
        if (credentialsProvider != null) {
            return true;
        }
        return (StringUtils.hasText(username) && !"${security.username}".equals(username)) &&
                (StringUtils.hasText(password) && !"${security.password}".equals(password));
    }

    public static SecurityConfig fromMarshalledProperties(Properties properties) {
        CredentialsProvider credentials = CredentialsProviderHelper.extractMarshalledCredentials(properties, false);
        return credentials == null ? null : new SecurityConfig(credentials);
    }
}
