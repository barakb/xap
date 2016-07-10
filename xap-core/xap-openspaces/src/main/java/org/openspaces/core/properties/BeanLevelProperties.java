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


package org.openspaces.core.properties;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * An extension to Spring support for properties based configuration. This is a properties holder
 * that can have both context level properties and bean specific (mapped based on the bean name)
 * properties.
 *
 * @author kimchy
 * @see BeanLevelPropertiesAware
 * @see BeanLevelPropertyPlaceholderConfigurer
 */
public class BeanLevelProperties implements Serializable {

    private static final long serialVersionUID = -5373882281270584863L;

    private Properties contextProperties = new Properties();

    private Map<String, Properties> beanProperties = new HashMap<String, Properties>();

    /**
     * Returns the Spring context level properties.
     */
    public Properties getContextProperties() {
        return contextProperties;
    }

    /**
     * Sets the Spring context level properties.
     */
    public void setContextProperties(Properties contextProperties) {
        this.contextProperties = contextProperties;
    }

    /**
     * Returns properties that are associated with a specific bean name. If the properties do not
     * exists, they will be created and bounded to the bean name.
     *
     * @param beanName The bean name to get the properties for
     * @return The properties assigned to the bean
     */
    public Properties getBeanProperties(String beanName) {
        Properties props = beanProperties.get(beanName);
        if (props == null) {
            props = new Properties();
            beanProperties.put(beanName, props);
        }
        return props;
    }

    /**
     * Sets properties that will be associated with the given bean name.
     *
     * @param beanName            The bean name to bind the properties to
     * @param nameBasedProperties The properties associated with the bean name
     */
    public void setBeanProperties(String beanName, Properties nameBasedProperties) {
        this.beanProperties.put(beanName, nameBasedProperties);
    }

    /**
     * Returns <code>true</code> if there are properties associated with the given bean name.
     *
     * @param beanName The bean name to check if there are properties associated with it
     * @return <code>true</code> if there are properties associated with the bean name
     */
    public boolean hasBeanProperties(String beanName) {
        return getBeanProperties(beanName).size() != 0;
    }

    /**
     * Returns merged properties between the Spring context level properties and the bean level
     * properties associated with the provided bean name. The bean level properties will override
     * existing properties defined in the context level properties.
     *
     * @param beanName The bean name to return the merged properties for
     * @return Merged properties of the bean level properties and context properties for the given
     * bean name
     */
    public Properties getMergedBeanProperties(String beanName) {
        Properties props = new Properties();
        props.putAll(contextProperties);
        Properties nameBasedProperties = getBeanProperties(beanName);
        if (nameBasedProperties != null) {
            props.putAll(nameBasedProperties);
        }
        return props;
    }

    public Map<String, Properties> getBeans() {
        return Collections.synchronizedMap(beanProperties);
    }

    public String toString() {
        return "Context " + SanitizeUtil.sanitize(contextProperties, "password", "EncryptionKey")
                + " Beans " + SanitizeUtil.sanitize(beanProperties, "password", "EncryptionKey");
    }
}
