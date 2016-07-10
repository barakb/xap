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

package org.openspaces.core.space;

import com.gigaspaces.cluster.activeelection.LeaderSelectorConfig;

import org.springframework.beans.factory.InitializingBean;

/**
 * @author kobi on 11/22/15.
 * @since 10.2
 */
public class LeaderSelectorFactoryBean implements InitializingBean {

    protected LeaderSelectorConfig config;


    public LeaderSelectorConfig getConfig() {
        return config;
    }

    public void setConfig(LeaderSelectorConfig config) {
        this.config = config;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }

}
