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

package org.openspaces.leader_selector.zookeeper.config;

import com.j_spaces.core.Constants;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * @author kobi on 11/23/15.
 * @since 11.0
 */
public class ZooKeeperBeanDefinitionParser extends AbstractSimpleBeanDefinitionParser {

    private static final String SESSION_TIMEOUT = "session-timeout";
    private static final String CONNECTION_TIMEOUT = "connection-timeout";
    private static final String RETRIES = "retries";
    private static final String SLEEP_BETWEEN_RETRIES = "sleep-between-retries";

    @Override
    protected Class<ZooKeeperLeaderSelectorFactoryBean> getBeanClass(Element element) {
        return ZooKeeperLeaderSelectorFactoryBean.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        final String sessionTimeout = element.getAttribute(SESSION_TIMEOUT);
        if (StringUtils.hasText(sessionTimeout))
            builder.addPropertyValue("sessionTimeout", sessionTimeout);
        else
            builder.addPropertyValue("sessionTimeout", Constants.LeaderSelector.ZOOKEEPER.CURATOR_SESSION_TIMEOUT_DEFAULT);

        final String connectionTimeout = element.getAttribute(CONNECTION_TIMEOUT);
        if (StringUtils.hasText(connectionTimeout))
            builder.addPropertyValue("connectionTimeout", connectionTimeout);
        else
            builder.addPropertyValue("connectionTimeout", Constants.LeaderSelector.ZOOKEEPER.CURATOR_CONNECTION_TIMEOUT_DEFAULT);

        final String retries = element.getAttribute(RETRIES);
        if (StringUtils.hasText(retries))
            builder.addPropertyValue("retries", retries);
        else
            builder.addPropertyValue("retries", Constants.LeaderSelector.ZOOKEEPER.CURATOR_RETRIES_DEFAULT);

        final String sleepMsBetweenRetries = element.getAttribute(SLEEP_BETWEEN_RETRIES);
        if (StringUtils.hasText(sleepMsBetweenRetries))
            builder.addPropertyValue("sleepMsBetweenRetries", sleepMsBetweenRetries);
        else
            builder.addPropertyValue("sleepMsBetweenRetries", Constants.LeaderSelector.ZOOKEEPER.CURATOR_SLEEP_MS_BETWEEN_RETRIES_DEFAULT);
    }
}
