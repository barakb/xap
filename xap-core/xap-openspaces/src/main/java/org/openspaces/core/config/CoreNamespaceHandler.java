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

import org.openspaces.core.config.modifiers.SpaceProxyOperationModifierBeanDefinitionParser;
import org.openspaces.core.extension.OpenSpacesExtensions;
import org.openspaces.core.transaction.config.DistributedTransactionProcessingConfigurationBeanDefinitionParser;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

import java.util.Map;

/**
 * A Spring name space handler for OpenSpaces core package.
 *
 * @author kimchy
 */
public class CoreNamespaceHandler extends NamespaceHandlerSupport {

    public void init() {
        registerBeanDefinitionParser("rest", new RestBeanDefinitionParser());
        registerBeanDefinitionParser("space", new UrlSpaceBeanDefinitionParser());
        registerBeanDefinitionParser("embedded-space", new EmbeddedSpaceBeanDefinitionParser());
        registerBeanDefinitionParser("space-proxy", new SpaceProxyBeanDefinitionParser());
        registerBeanDefinitionParser("sql-query", new SQLQueryBeanDefinitionParser());
        registerBeanDefinitionParser("view-query", new SQLQueryBeanDefinitionParser());

        registerBeanDefinitionParser("giga-space", new GigaSpaceBeanDefinitionParser());
        registerBeanDefinitionParser("local-tx-manager", new LocalTxManagerBeanDefinitionParser());
        registerBeanDefinitionParser("jini-tx-manager", new LookupJiniTxManagerBeanDefinitionParser());
        registerBeanDefinitionParser("distributed-tx-manager", new DistributedTxManagerBeanDefinitionParser());
        try {
            registerBeanDefinitionParser("giga-space-context", new GigaSpaceContextBeanDefinitionParser());
            registerBeanDefinitionParser("giga-space-late-context", new GigaSpaceLateContextBeanDefinitionParser());
        } catch (Throwable t) {
            // do nothing, working under 1.4
        }
        registerBeanDefinitionParser("context-loader", new ContextLoaderBeanDefinitionParser());
        registerBeanDefinitionParser("refreshable-context-loader", new RefreshableContextLoaderBeanDefinitionParser());

        registerBeanDefinitionParser("space-filter", new SpaceFilterBeanDefinitionParser());
        registerBeanDefinitionParser("annotation-adapter-filter", new AnnotationFilterBeanDefinitionParser());
        registerBeanDefinitionParser("method-adapter-filter", new MethodFilterBeanDefinitionParser());

        registerBeanDefinitionParser("space-replication-filter", new SpaceReplicationFilterBeanDefinitionParser());
        registerBeanDefinitionParser("space-sql-function", new SqlFunctionBeanDefinitionParser());

        registerBeanDefinitionParser("annotation-support", new AnnotationSupportBeanDefinitionParser());
        registerBeanDefinitionParser("space-type", new GigaSpaceDocumentTypeBeanDefinitionParser());
        registerBeanDefinitionParser("mirror", new MirrorSpaceBeanDefinitionParser());
        registerBeanDefinitionParser("tx-support", new DistributedTransactionProcessingConfigurationBeanDefinitionParser());
        registerBeanDefinitionParser("custom-cache-policy", new CustomCachePolicyBeanDefinitionParser());
        registerBeanDefinitionParser("blob-store-data-policy", new BlobStoreDataPolicyBeanDefinitionParser());
        registerBeanDefinitionParser("attribute-store", new AttributeStoreBeanDefinitionParser());
        registerBeanDefinitionParser("leader-selector", new LeaderSelectorBeanDefinitionParser());

        SpaceProxyOperationModifierBeanDefinitionParser defaultModifiersParser =
                new SpaceProxyOperationModifierBeanDefinitionParser();
        registerBeanDefinitionParser("write-modifier", defaultModifiersParser);
        registerBeanDefinitionParser("read-modifier", defaultModifiersParser);
        registerBeanDefinitionParser("take-modifier", defaultModifiersParser);
        registerBeanDefinitionParser("count-modifier", defaultModifiersParser);
        registerBeanDefinitionParser("clear-modifier", defaultModifiersParser);
        registerBeanDefinitionParser("change-modifier", defaultModifiersParser);

        for (Map.Entry<String, BeanDefinitionParser> entry : OpenSpacesExtensions.getInstance().getCoreBeanDefinitionParsers().entrySet()) {
            registerBeanDefinitionParser(entry.getKey(), entry.getValue());
        }
    }

}
