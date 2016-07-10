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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * Created by Tamir on 3/6/16.
 *
 * @since 11.0
 */
public class SpaceSqlFunctionBean implements InitializingBean, BeanNameAware {

    private static final Log logger = LogFactory.getLog(SpaceSqlFunctionBean.class);

    private String beanName;
    private String functionName;
    private Object sqlFunction;

    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }

    public String getBeanName() {
        return beanName;
    }

    public Object getSqlFunction() {
        return sqlFunction;
    }

    public void setSqlFunction(Object sqlFunction) {
        this.sqlFunction = sqlFunction;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(beanName, "beanName property is required");
        Assert.notNull(functionName, "functionName property is required");
        Assert.isTrue(!functionName.isEmpty(), "functionName property must be not empty");
        Assert.notNull(sqlFunction, "sqlFunction property is required");
    }
}
