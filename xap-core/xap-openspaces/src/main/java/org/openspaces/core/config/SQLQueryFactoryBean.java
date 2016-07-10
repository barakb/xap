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

import com.j_spaces.core.client.SQLQuery;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * A helper factory beans for {@link com.j_spaces.core.client.SQLQuery} so namespace based
 * configuration will be simpler.
 *
 * @author kimchy
 */
public class SQLQueryFactoryBean implements FactoryBean, InitializingBean {

    private static final String PROJECTIONS_SEPARATOR = ",";

    private String where;

    private Object template;

    private Class<Object> type;

    private String className;

    private SQLQuery<Object> sqlQuery;

    private String projections;

    public void setWhere(String where) {
        this.where = where;
    }

    protected String getWhere() {
        return this.where;
    }

    public void setTemplate(Object template) {
        this.template = template;
    }

    protected Object getTemplate() {
        return this.template;
    }

    public void setType(Class<Object> clazz) {
        this.type = clazz;
    }

    protected Class<Object> getType() {
        return this.type;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    protected String getClassName() {
        return this.className;
    }

    public String getProjections() {
        return this.projections;
    }

    public void setProjections(String projections) {
        this.projections = projections;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        validate();
        if (getTemplate() != null) {
            sqlQuery = new SQLQuery<Object>(getTemplate(), getWhere());
        } else if (type != null) {
            sqlQuery = new SQLQuery<Object>(getType(), getWhere());
        } else {
            sqlQuery = new SQLQuery<Object>(getClassName(), getWhere());
        }
        if (projections != null) {
            String[] projectionsArray = projections.split(PROJECTIONS_SEPARATOR);
            for (int i = 0; i < projectionsArray.length; i++)
                projectionsArray[i] = projectionsArray[i].trim();
            sqlQuery.setProjections(projectionsArray);
        }
    }

    protected void validate() throws IllegalArgumentException {
        Assert.notNull(where, "where property is required");
        if (getTemplate() == null && getType() == null && getClassName() == null) {
            throw new IllegalArgumentException("either template property or type property or className must be set");
        }
    }

    @Override
    public Object getObject() throws Exception {
        return this.sqlQuery;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<SQLQuery> getObjectType() {
        return SQLQuery.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
