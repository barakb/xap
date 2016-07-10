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

package com.j_spaces.jmx;

import com.j_spaces.core.IJSpace;
import com.j_spaces.core.admin.SpaceConfig;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import javax.management.RuntimeOperationsException;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
@com.gigaspaces.api.InternalApi
public class JMXSpaceExt extends XMLDescriptorsMBean {
    private SpaceConfig m_spaceConfig;

    /**
     * @param spaceProxy
     * @param spaceConfig
     * @param xmlDescriptorsFileURL
     * @throws Exception
     */
    public JMXSpaceExt(IJSpace spaceProxy, SpaceConfig spaceConfig, String xmlDescriptorsFileURL) throws Exception {
        super(xmlDescriptorsFileURL);
        // Obtain the embedded space proxy
        __setConfig(spaceConfig);
    }

    @Override
    protected void __setConfig(Object config) {
        m_spaceConfig = (SpaceConfig) config;
    }

    @Override
    protected Object __getConfig() {
        return m_spaceConfig;
    }

    /**
     * Allows the value of the specified attribute of the Dynamic MBean to be obtained.
     */
    public Object getAttribute(String attributeName)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
        if (attributeName == null || attributeName.trim().length() <= 0)
            throw new RuntimeOperationsException((new IllegalArgumentException
                    ("Attribute name can not be null or empty")),
                    "Unable to retrieve property from " + THIS_CLASS_NAME +
                            " with a null or empty attribute name string.");

        Object result = m_spaceConfig.getProperty(com.j_spaces.core.Constants.SPACE_CONFIG_PREFIX + attributeName);

        //try to receive value from parent class if such key does not exist
        if (result == null)
            result = super.getAttribute(attributeName);
        return result;
    }
}
