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

package com.gigaspaces.grid.gsa;

import com.gigaspaces.api.InternalApi;

/**
 * @author kimchy
 */
@InternalApi
public class AgentHelper {

    public static boolean hasAgentId() {
        return getAgentId() > -1;
    }

    public static int getAgentId() {
        String agentId = System.getProperty("agentId");
        if (agentId == null) {
            agentId = System.getenv("AGENT_ID");
        }
        if (agentId == null || agentId.trim().length() == 0) {
            return -1;
        }
        return Integer.parseInt(agentId);
    }

    public static String getGSAServiceID() {
        String retVal = System.getProperty("gsaServiceID");
        if (retVal == null) {
            retVal = System.getenv("GSA_SERVICE_ID");
        }
        return retVal;
    }

    public static boolean enableDynamicLocators() {
        String value = System.getProperty("enableDynamicLocators");
        if (value == null) {
            value = System.getenv("ENABLE_DYNAMIC_LOCATORS");
        }
        return Boolean.parseBoolean(value);
    }

}
