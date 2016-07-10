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

import com.j_spaces.core.IJSpace;

import org.openspaces.core.config.SpaceSqlFunctionBean;
import org.openspaces.core.properties.BeanLevelMergedPropertiesAware;
import org.springframework.dao.DataAccessException;

import java.util.Properties;

/**
 * @author yuvalm
 * @since 10.0
 */
public class SpaceProxyFactoryBean extends AbstractSpaceFactoryBean implements BeanLevelMergedPropertiesAware {

    private final InternalSpaceFactory factory = new InternalSpaceFactory();
    private String name;

    public SpaceProxyFactoryBean() {
    }

    public SpaceProxyFactoryBean(String name) {
        this();
        setName(name);
    }

    @Override
    protected IJSpace doCreateSpace() throws DataAccessException {
        return factory.create(this, name, true);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSpaceName(String name) {
        this.name = name;
    }

    public void setInstanceId(String instanceId) {
        factory.getFactory().setInstanceId(instanceId);
    }

    @Override
    public void setSecurityConfig(SecurityConfig securityConfig) {
        super.setSecurityConfig(securityConfig);
        factory.setSecurityConfig(securityConfig);
    }

    public void setProperties(Properties properties) {
        factory.getFactory().setProperties(properties);
    }

    public void setLookupGroups(String lookupGroups) {
        factory.getFactory().setLookupGroups(lookupGroups);
    }

    public void setLookupLocators(String lookupLocators) {
        factory.getFactory().setLookupLocators(lookupLocators);
    }

    public void setLookupTimeout(int lookupTimeout) {
        factory.getFactory().setLookupTimeout(lookupTimeout);
    }

    public void setVersioned(boolean versioned) {
        factory.getFactory().setVersioned(versioned);
    }

    public void setSpaceSqlFunction(SpaceSqlFunctionBean[] spaceSqlFunctionBeans) {
        factory.setSpaceSqlFunction(spaceSqlFunctionBeans);
    }

    @Override
    public void setMergedBeanLevelProperties(Properties beanLevelProperties) {
        factory.getFactory().setBeanLevelProperties(beanLevelProperties);
    }
}
