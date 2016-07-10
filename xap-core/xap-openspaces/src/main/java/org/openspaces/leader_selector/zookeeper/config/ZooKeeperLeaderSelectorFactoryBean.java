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

import com.gigaspaces.cluster.activeelection.LeaderSelectorConfig;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * @author kobi on 11/22/15.
 * @since 11.0
 */
public class ZooKeeperLeaderSelectorFactoryBean implements InitializingBean, FactoryBean {

    protected LeaderSelectorConfig config;
    private long sessionTimeout;
    private long connectionTimeout;
    private int retries;
    private int sleepMsBetweenRetries;

    public void setSessionTimeout(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public void setConnectionTimeout(long connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public void setSleepMsBetweenRetries(int sleepMsBetweenRetries) {
        this.sleepMsBetweenRetries = sleepMsBetweenRetries;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        config = new LeaderSelectorConfig();
        config.getProperties().setProperty("leaderSelectorHandler", "org.openspaces.zookeeper.leader_selector.ZooKeeperBasedLeaderSelectorHandler");
        config.getProperties().setProperty("sessionTimeout", String.valueOf(sessionTimeout));
        config.getProperties().setProperty("connectionTimeout", String.valueOf(connectionTimeout));
        config.getProperties().setProperty("retries", String.valueOf(retries));
        config.getProperties().setProperty("sleepMsBetweenRetries", String.valueOf(sleepMsBetweenRetries));
    }

    @Override
    public Object getObject() throws Exception {
        return config;
    }

    @Override
    public Class<?> getObjectType() {
        return (config == null ? LeaderSelectorConfig.class : config.getClass());
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
