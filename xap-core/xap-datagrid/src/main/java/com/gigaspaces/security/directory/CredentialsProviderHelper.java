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

import com.gigaspaces.internal.utils.StringUtils;
import com.j_spaces.core.Constants;

import java.io.IOException;
import java.rmi.MarshalledObject;
import java.util.Properties;

@com.gigaspaces.api.InternalApi
public class CredentialsProviderHelper {

    private static final String USER_DETAILS_PROPERTY = Constants.Security.USER_DETAILS;
    private static final String CREDENTIALS_PROVIDER_PROPERTY = Constants.Security.CREDENTIALS_PROVIDER;

    public static CredentialsProvider fromClassName(String className, Properties properties)
            throws Exception {
        Object instance;
        if (StringUtils.hasLength(className)) {
            Class clazz = Class.forName(className);
            instance = clazz.newInstance();
        } else
            instance = new DefaultCredentialsProvider();

        if (instance instanceof CredentialsProvider) {
            CredentialsProvider result = (CredentialsProvider) instance;
            result.initialize(properties);
            return (CredentialsProvider) instance;
        }

        if (instance instanceof UserDetailsProvider) {
            UserDetailsProvider userDetailsProvider = (UserDetailsProvider) instance;
            UserDetails userDetails = userDetailsProvider.create(properties);
            return new DefaultCredentialsProvider(userDetails);
        }

        throw new IllegalStateException("Class '" + className + "' is not a valid credentials provider implementation.");
    }

    public static Properties createLoginProperties(String username, String password) {
        Properties properties = new Properties();
        properties.setProperty(CredentialsProvider.USERNAME_PROPERTY, username);
        properties.setProperty(CredentialsProvider.PASSWORD_PROPERTY, password);
        return properties;
    }

    public static void clearCredentialsProperties(Properties properties) {
        properties.remove(USER_DETAILS_PROPERTY);
        properties.remove(CREDENTIALS_PROVIDER_PROPERTY);
    }

    public static void appendCredentials(Properties properties, CredentialsProvider credentialsProvider) {
        properties.put(CREDENTIALS_PROVIDER_PROPERTY, credentialsProvider);
    }

    public static CredentialsProvider extractCredentials(Properties properties) {

        CredentialsProvider credentials = extract(properties, CREDENTIALS_PROVIDER_PROPERTY, true);
        if (credentials != null)
            return credentials;

        UserDetails userDetails = extract(properties, USER_DETAILS_PROPERTY, true);
        if (userDetails != null)
            return new DefaultCredentialsProvider(userDetails);

        return null;
    }

    public static void appendMarshalledCredentials(Properties properties, UserDetails userDetails, CredentialsProvider credentialsProvider)
            throws IOException {
        if (credentialsProvider != null)
            properties.put(CREDENTIALS_PROVIDER_PROPERTY, new MarshalledObject<CredentialsProvider>(credentialsProvider));
        if (userDetails != null)
            properties.put(USER_DETAILS_PROPERTY, new MarshalledObject<UserDetails>(userDetails));
    }

    public static CredentialsProvider extractMarshalledCredentials(Properties properties, boolean remove) {

        try {
            MarshalledObject<CredentialsProvider> credentials = extract(properties, CREDENTIALS_PROVIDER_PROPERTY, remove);
            if (credentials != null)
                return credentials.get();

            MarshalledObject<UserDetails> userDetails = extract(properties, USER_DETAILS_PROPERTY, remove);
            if (userDetails != null)
                return new DefaultCredentialsProvider(userDetails.get());

            return null;
        } catch (ClassNotFoundException e) {
            throw new com.gigaspaces.security.SecurityException("Failed to extract credentials", e);
        } catch (IOException e) {
            throw new com.gigaspaces.security.SecurityException("Failed to extract credentials", e);
        }
    }

    private static <T> T extract(Properties properties, String key, boolean remove) {
        Object result = remove ? properties.remove(key) : properties.get(key);
        return (T) result;
    }
}
