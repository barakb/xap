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

import com.j_spaces.core.Constants;

import org.openspaces.core.space.UrlSpaceFactoryBean;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedProperties;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import java.util.Properties;

/**
 * A bean definition builder for mirror space {@link UrlSpaceFactoryBean}.
 *
 * @author anna
 */
public class MirrorSpaceBeanDefinitionParser extends AbstractSimpleBeanDefinitionParser {

    public static final String DATA_SOURCE = "external-data-source";
    public static final String SOURCE_SPACE = "source-space";
    public static final String OPERATION_GROUPING = "operation-grouping";
    public static final String PROPERTIES = "properties";
    public static final String TRANSACTION_SUPPORT = "tx-support";
    public static final String SPACE_SYNC_ENDPOINT = "space-sync-endpoint";

    @Override
    protected Class<UrlSpaceFactoryBean> getBeanClass(Element element) {
        return UrlSpaceFactoryBean.class;
    }

    @Override
    protected boolean isEligibleAttribute(String attributeName) {
        return super.isEligibleAttribute(attributeName) && !DATA_SOURCE.equals(attributeName)
                && !SPACE_SYNC_ENDPOINT.equals(attributeName) && !OPERATION_GROUPING.equals(attributeName);
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);

        builder.addPropertyValue("schema", "mirror");

        //parse the external-data-source attribute
        String dataSource = element.getAttribute(DATA_SOURCE);
        if (StringUtils.hasLength(dataSource)) {
            builder.addPropertyReference("externalDataSource", dataSource);
        }

        //parse the space-sync-endpoint attribute
        String spaceSynchronizationEndpoint = element.getAttribute(SPACE_SYNC_ENDPOINT);
        if (StringUtils.hasLength(spaceSynchronizationEndpoint)) {
            builder.addPropertyReference("spaceSynchronizationEndpoint", spaceSynchronizationEndpoint);
        }

        //parse the operation-grouping attribute
        Properties properties = new Properties();
        String operationGrouping = element.getAttribute(OPERATION_GROUPING);
        if (StringUtils.hasLength(operationGrouping)) {
            properties.put(Constants.Mirror.FULL_MIRROR_SERVICE_OPERATION_GROUPING_TAG, operationGrouping);
        }


        //parse the source-space element
        Element sourceSpaceEle = DomUtils.getChildElementByTagName(element, SOURCE_SPACE);
        if (sourceSpaceEle != null) {

            properties.put(Constants.Mirror.FULL_MIRROR_SERVICE_CLUSTER_NAME, sourceSpaceEle.getAttribute("name"));
            properties.put(Constants.Mirror.FULL_MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT, sourceSpaceEle.getAttribute("partitions"));
            properties.put(Constants.Mirror.FULL_MIRROR_SERVICE_CLUSTER_BACKUPS_PER_PARTITION, sourceSpaceEle.getAttribute("backups"));
        }

        builder.addPropertyValue("properties", properties);

        Element propertiesEle = DomUtils.getChildElementByTagName(element, PROPERTIES);
        ManagedProperties spaceProps = new ManagedProperties();
        if (propertiesEle != null) {
            spaceProps = (ManagedProperties) parserContext.getDelegate().parsePropertyValue(propertiesEle,
                    builder.getRawBeanDefinition(), "properties");

            spaceProps.setMergeEnabled(true);
            builder.addPropertyValue("properties", spaceProps);

        }

        //check if external-data-source is defined in a nested bean
        Element edsEle = DomUtils.getChildElementByTagName(element, DATA_SOURCE);
        if (edsEle != null) {
            Assert.state(!StringUtils.hasText(dataSource),
                    "duplicate definition of external-data-source");

            Object eds = parserContext.getDelegate().parsePropertyValue(edsEle,
                    builder.getRawBeanDefinition(), "externalDataSource");
            builder.addPropertyValue("externalDataSource", eds);
        }

        // Distributed transaction processing parameters (since 8.0.4)
        final Element transactionProcessingConfigurationElement = DomUtils.getChildElementByTagName(element,
                TRANSACTION_SUPPORT);
        if (transactionProcessingConfigurationElement != null) {
            Object transactionProcessingConfiguration = parserContext.getDelegate().parsePropertySubElement(transactionProcessingConfigurationElement, builder.getRawBeanDefinition());
            builder.addPropertyValue("distributedTransactionProcessingConfiguration", transactionProcessingConfiguration);
        }

    }
}
