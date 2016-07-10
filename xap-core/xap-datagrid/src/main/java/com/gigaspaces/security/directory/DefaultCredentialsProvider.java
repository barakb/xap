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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

/**
 * @author Niv Ingberg
 * @since 9.5.1
 */

public class DefaultCredentialsProvider extends CredentialsProvider {

    private static final long serialVersionUID = 1L;

    private UserDetails credentials;

    /**
     * Required for Externalizable
     */
    public DefaultCredentialsProvider() {
    }

    public DefaultCredentialsProvider(UserDetails credentials) {
        this.credentials = credentials;
    }

    public DefaultCredentialsProvider(String username, String password) {
        this(create(username, password));
    }

    @Override
    public void initialize(Properties properties) {
        super.initialize(properties);
        String username = getProperty(properties, USERNAME_PROPERTY);
        String password = getProperty(properties, PASSWORD_PROPERTY);
        this.credentials = create(username, password);
    }

    @Override
    public UserDetails getUserDetails() {
        return credentials;
    }

    private static String getProperty(Properties properties, String key) {
        if (!properties.containsKey(key))
            throw new IllegalArgumentException("Property '" + key + "' is not defined");
        String value = properties.getProperty(key);
        if (!StringUtils.hasLength(value))
            throw new IllegalArgumentException("Property '" + key + "' is null or empty");
        return value;
    }

    private static UserDetails create(String username, String password) {
        return new User(username, password);
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);
        out.writeObject(credentials);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.credentials = (UserDetails) in.readObject();
    }
}
