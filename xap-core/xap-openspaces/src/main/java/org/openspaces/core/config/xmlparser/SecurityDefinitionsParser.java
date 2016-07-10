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

package org.openspaces.core.config.xmlparser;

import org.openspaces.core.space.SecurityConfig;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * @author Niv Ingberg
 */
public class SecurityDefinitionsParser {

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String USER_DETAILS = "user-details";
    private static final String CREDENTIALS_PROVIDER = "credentials-provider";

    public static void parseXml(Element securityElement, BeanDefinitionBuilder builder) {
        final String username = securityElement.getAttribute(USERNAME);
        final String password = securityElement.getAttribute(PASSWORD);
        if (StringUtils.hasText(username)) {
            SecurityConfig securityConfig = new SecurityConfig();
            securityConfig.setUsername(username);
            if (StringUtils.hasText(password)) {
                securityConfig.setPassword(password);
            }
            if (securityConfig != null)
                builder.addPropertyValue("securityConfig", securityConfig);
        }

        final String userDetailsRef = securityElement.getAttribute(USER_DETAILS);
        if (StringUtils.hasText(userDetailsRef)) {
            builder.addPropertyReference("userDetails", userDetailsRef);
        }

        // Since 9.1.2-patch3 
        String credentialsProviderRef = securityElement.getAttribute(CREDENTIALS_PROVIDER);
        if (StringUtils.hasText(credentialsProviderRef)) {
            builder.addPropertyReference("credentialsProvider", credentialsProviderRef);
        }
    }
}
