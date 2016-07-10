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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;


/**
 * @author Kobi
 * @since 10.0.0
 */
public class BlobStoreDataPolicyBeanDefinitionParser extends AbstractSimpleBeanDefinitionParser {

    private static final String AVG_OBJECT_SIZE_KB = "avg-object-size-KB";
    private static final String AVG_OBJECT_SIZE_BYTES = "avg-object-size-bytes";
    private static final String CACHE_ENTRIES_PERCENTAGE = "cache-entries-percentage";
    private static final String PERSISTENT = "persistent";
    private static final String STORAGE_HANDLER = "blob-store-handler";

    @Override
    protected Class<BlobStoreDataPolicyFactoryBean> getBeanClass(Element element) {
        return BlobStoreDataPolicyFactoryBean.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        final String avgObjectSizeKB = element.getAttribute(AVG_OBJECT_SIZE_KB);
        final String avgObjectSizeBytes = element.getAttribute(AVG_OBJECT_SIZE_BYTES);
        if (StringUtils.hasText(avgObjectSizeKB) && StringUtils.hasText(avgObjectSizeBytes)) {
            throw new IllegalArgumentException("avg-object-size-KB and avg-object-size-bytes cannot be used together");
        }
        if (StringUtils.hasText(avgObjectSizeKB)) {
            builder.addPropertyValue("avgObjectSizeKB", avgObjectSizeKB);
        }

        if (StringUtils.hasText(avgObjectSizeBytes))
            builder.addPropertyValue("avgObjectSizeBytes", avgObjectSizeBytes);

        final String cacheEntriesPercentage = element.getAttribute(CACHE_ENTRIES_PERCENTAGE);
        if (StringUtils.hasText(cacheEntriesPercentage))
            builder.addPropertyValue("cacheEntriesPercentage", cacheEntriesPercentage);

        final String persistent = element.getAttribute(PERSISTENT);
        if (StringUtils.hasText(persistent))
            builder.addPropertyValue("persistent", persistent);

        final String blobStoreStorageHandler = element.getAttribute(STORAGE_HANDLER);
        if (StringUtils.hasText(blobStoreStorageHandler))
            builder.addPropertyReference("blobStoreHandler", blobStoreStorageHandler);

        if (!StringUtils.hasText(blobStoreStorageHandler))
            throw new IllegalArgumentException("A reference to a space blob store handler bean must be specified");
    }

}
