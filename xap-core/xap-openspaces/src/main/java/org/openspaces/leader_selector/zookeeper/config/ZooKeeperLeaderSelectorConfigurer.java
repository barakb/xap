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
import com.j_spaces.core.Constants;

import java.util.concurrent.TimeUnit;

/**
 * @author kobi on 11/22/15.
 * @since 10.2
 */
public class ZooKeeperLeaderSelectorConfigurer {
    private final LeaderSelectorConfig leaderSelectorConfig;

    public ZooKeeperLeaderSelectorConfigurer() {
        leaderSelectorConfig = new LeaderSelectorConfig();
        leaderSelectorConfig.getProperties().setProperty("leaderSelectorHandler", Constants.LeaderSelector.ZOOKEEPER.LEADER_SELECTOR_HANDLER_CLASS_NAME);
        leaderSelectorConfig.getProperties().setProperty("sessionTimeout", String.valueOf(Constants.LeaderSelector.ZOOKEEPER.CURATOR_SESSION_TIMEOUT_DEFAULT));
        leaderSelectorConfig.getProperties().setProperty("connectionTimeout", String.valueOf(Constants.LeaderSelector.ZOOKEEPER.CURATOR_CONNECTION_TIMEOUT_DEFAULT));
        leaderSelectorConfig.getProperties().setProperty("retries", String.valueOf(Constants.LeaderSelector.ZOOKEEPER.CURATOR_RETRIES_DEFAULT));
        leaderSelectorConfig.getProperties().setProperty("sleepMsBetweenRetries", String.valueOf(Constants.LeaderSelector.ZOOKEEPER.CURATOR_SLEEP_MS_BETWEEN_RETRIES_DEFAULT));
    }

    /**
     * @param sessionTimeout - curator client session timeout
     * @return itself
     */
    public ZooKeeperLeaderSelectorConfigurer sessionTimeout(long sessionTimeout, TimeUnit timeUnit) {
        leaderSelectorConfig.getProperties().setProperty("sessionTimeout", String.valueOf(timeUnit.toMillis(sessionTimeout)));
        return this;
    }

    /**
     * @param connectionTimeout - curator client connection timeout
     * @return itself
     */
    public ZooKeeperLeaderSelectorConfigurer connectionTimeout(long connectionTimeout, TimeUnit timeUnit) {
        leaderSelectorConfig.getProperties().setProperty("connectionTimeout", String.valueOf(timeUnit.toMillis(connectionTimeout)));
        return this;
    }

    /**
     * @param retries - curator client retries
     * @return itself
     */
    public ZooKeeperLeaderSelectorConfigurer retries(long retries) {
        leaderSelectorConfig.getProperties().setProperty("retries", String.valueOf(retries));
        return this;
    }

    /**
     * @param sleepMsBetweenRetries - curator client sleep between retries
     * @return itself
     */
    public ZooKeeperLeaderSelectorConfigurer sleepMsBetweenRetries(long sleepMsBetweenRetries, TimeUnit timeUnit) {
        leaderSelectorConfig.getProperties().setProperty("sleepMsBetweenRetries", String.valueOf(timeUnit.toMillis(sleepMsBetweenRetries)));
        return this;
    }

    public LeaderSelectorConfig create() {
        return leaderSelectorConfig;
    }

}
