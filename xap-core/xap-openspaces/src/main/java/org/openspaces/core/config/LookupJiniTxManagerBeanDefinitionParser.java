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


package org.openspaces.core.config;

import org.openspaces.core.transaction.manager.LookupJiniTransactionManager;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.core.Conventions;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;

/**
 * A bean definition builder for {@link LookupJiniTransactionManager}.
 *
 * @author kimchy
 */
public class LookupJiniTxManagerBeanDefinitionParser extends AbstractJiniTxManagerBeanDefinitionParser {

    public static final String TX_MANAGER_NAME = "tx-manager-name";

    public static final String GROUPS = "groups";

    public static final String LOCATORS = "locators";

    protected Class<LookupJiniTransactionManager> getBeanClass(Element element) {
        return LookupJiniTransactionManager.class;
    }

    protected void doParse(Element element, BeanDefinitionBuilder builder) {
        super.doParse(element, builder);
        NamedNodeMap attributes = element.getAttributes();
        for (int x = 0; x < attributes.getLength(); x++) {
            Attr attribute = (Attr) attributes.item(x);
            String name = attribute.getLocalName();
            if (ID_ATTRIBUTE.equals(name)) {
                continue;
            }
            String propertyName = extractPropertyName(name);
            if (TX_MANAGER_NAME.equals(name)) {
                builder.addPropertyValue("transactionManagerName", attribute.getValue());
                continue;
            }
            if (GROUPS.equals(name)) {
                String groups = attribute.getValue();
                if (StringUtils.hasText(groups)) {
                    String[] groupsArr = StringUtils.tokenizeToStringArray(groups, ",");
                    builder.addPropertyValue("groups", groupsArr);
                }
            }
            if (LOCATORS.equals(name)) {
                String locators = attribute.getValue();
                if (StringUtils.hasText(locators)) {
                    String[] locatorsArr = StringUtils.tokenizeToStringArray(locators, ",");
                    builder.addPropertyValue("locators", locatorsArr);
                }
            }

            Assert.state(StringUtils.hasText(propertyName),
                    "Illegal property name returned from 'extractPropertyName(String)': cannot be null or empty.");
            builder.addPropertyValue(propertyName, attribute.getValue());
        }
    }

    protected String extractPropertyName(String attributeName) {
        return Conventions.attributeNameToPropertyName(attributeName);
    }
}