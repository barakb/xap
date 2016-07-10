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

import org.openspaces.core.transaction.manager.TransactionLeaseRenewalConfig;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * @author kimchy
 */
public abstract class AbstractJiniTxManagerBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    protected void doParse(Element element, BeanDefinitionBuilder builder) {
        Element renewEle = DomUtils.getChildElementByTagName(element, "renew");
        if (renewEle != null) {
            TransactionLeaseRenewalConfig renewalConfig = new TransactionLeaseRenewalConfig();
            String duration = renewEle.getAttribute("duration");
            if (StringUtils.hasText(duration)) {
                renewalConfig.setRenewDuration(Long.parseLong(duration));
            }
            String rtt = renewEle.getAttribute("round-trip-time");
            if (StringUtils.hasText(rtt)) {
                renewalConfig.setRenewRTT(Long.parseLong(rtt));
            }
            String poolSize = renewEle.getAttribute("pool-size");
            if (StringUtils.hasText(poolSize)) {
                renewalConfig.setPoolSize(Integer.parseInt(poolSize));
            }
            builder.addPropertyValue("leaseRenewalConfig", renewalConfig);
        }
    }
}
