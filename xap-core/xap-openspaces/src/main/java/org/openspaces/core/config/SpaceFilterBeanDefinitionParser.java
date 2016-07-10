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

import org.openspaces.core.space.filter.SpaceFilterProviderFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kimchy
 */
public class SpaceFilterBeanDefinitionParser extends AbstractFilterBeanDefinitionParser {

    protected Class getBeanClass(Element element) {
        return SpaceFilterProviderFactory.class;
    }

    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);
        List<Element> opeationCodesElements = DomUtils.getChildElementsByTagName(element, "operation");
        ArrayList<Integer> operationCodes = new ArrayList<Integer>();
        ArrayList<String> operationCodesNames = new ArrayList<String>();
        for (int i = 0; i < opeationCodesElements.size(); i++) {
            String operationCode = opeationCodesElements.get(i).getAttribute("code");
            if (StringUtils.hasText(operationCode)) {
                operationCodes.add(Integer.valueOf(operationCode));
            }
            operationCode = opeationCodesElements.get(i).getAttribute("code-name");
            if (StringUtils.hasText(operationCode)) {
                operationCodesNames.add(operationCode);
            }
        }
        int[] numberOperationCodes = new int[operationCodes.size()];
        for (int i = 0; i < numberOperationCodes.length; i++) {
            numberOperationCodes[i] = operationCodes.get(i);
        }
        builder.addPropertyValue("operationCodes", numberOperationCodes);

        builder.addPropertyValue("operationCodesNames", operationCodesNames.toArray(new String[operationCodesNames.size()]));
    }
}
