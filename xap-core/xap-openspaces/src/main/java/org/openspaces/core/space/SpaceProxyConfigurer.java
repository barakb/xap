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
import com.gigaspaces.security.directory.DefaultCredentialsProvider;
import com.j_spaces.core.IJSpace;

import org.springframework.util.StringUtils;

import java.util.Properties;

/**
 * @author yuvalm
 * @since 10.0
 */
public class SpaceProxyConfigurer extends AbstractSpaceConfigurer {

    private final SpaceProxyFactoryBean factoryBean;
    private final Properties properties = new Properties();

    public SpaceProxyConfigurer(String name) {
        factoryBean = new SpaceProxyFactoryBean(name);
    }

    @Override
    public void close() {
        factoryBean.close();
    }

    @Override
    protected IJSpace createSpace() {
        factoryBean.setProperties(properties);
        factoryBean.afterPropertiesSet();
        return (IJSpace) factoryBean.getObject();
    }

    public SpaceProxyConfigurer addProperty(String name, String value) {
        validate();
        properties.setProperty(name, value);
        return this;
    }

    public SpaceProxyConfigurer addProperties(Properties properties) {
        validate();
        this.properties.putAll(properties);
        return this;
    }

    public SpaceProxyConfigurer instanceId(String instanceID) {
        validate();
        factoryBean.setInstanceId(instanceID);
        return this;
    }

    public SpaceProxyConfigurer lookupGroups(String... lookupGroups) {
        validate();
        factoryBean.setLookupGroups(StringUtils.arrayToCommaDelimitedString(lookupGroups));
        return this;
    }

    public SpaceProxyConfigurer lookupGroups(String lookupGroups) {
        validate();
        factoryBean.setLookupGroups(lookupGroups);
        return this;
    }

    public SpaceProxyConfigurer lookupLocators(String lookupLocators) {
        validate();
        factoryBean.setLookupLocators(lookupLocators);
        return this;
    }

    public SpaceProxyConfigurer lookupLocators(String... lookupLocators) {
        validate();
        factoryBean.setLookupLocators(StringUtils.arrayToCommaDelimitedString(lookupLocators));
        return this;
    }

    public SpaceProxyConfigurer lookupTimeout(int lookupTimeout) {
        validate();
        factoryBean.setLookupTimeout(lookupTimeout);
        return this;
    }

    public SpaceProxyConfigurer versioned(boolean versioned) {
        validate();
        factoryBean.setVersioned(versioned);
        return this;
    }

    public SpaceProxyConfigurer credentials(String userName, String password) {
        return credentialsProvider(new DefaultCredentialsProvider(userName, password));
    }

    public SpaceProxyConfigurer credentialsProvider(CredentialsProvider credentialsProvider) {
        validate();
        factoryBean.setCredentialsProvider(credentialsProvider);
        return this;
    }
}
