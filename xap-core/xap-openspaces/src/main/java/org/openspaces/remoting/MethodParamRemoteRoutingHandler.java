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

package org.openspaces.remoting;

import java.util.HashMap;
import java.util.Map;

/**
 * A default implementation of routing handler allowing to control the routing index of the remote
 * invocation based on the remote method parameters.
 *
 * <p>Uses the {@link #setDefaultParamIndex(int)} as the default parameter index (starting from 0)
 * that will be used as the hash code. Also allows to configure per method param index by setting
 * {@link #setMethodParamIndex(java.util.Map)} (which has a method name as the key and the param
 * index as the value).
 *
 * @author kimchy
 */
public class MethodParamRemoteRoutingHandler implements RemoteRoutingHandler {

    private int defaultParamIndex = 0;

    private Map<String, Integer> methodParamIndex = new HashMap<String, Integer>();

    /**
     * The default paramter index (starting from 0) that will be used as the hash code to compute
     * the remote routing index.
     */
    public void setDefaultParamIndex(int defaultParamIndex) {
        this.defaultParamIndex = defaultParamIndex;
    }

    /**
     * Configures per method parameter index (starting from 0) that wil be used as the hash code to
     * compute the routing index. The map key is the method name and its value is the param index.
     */
    public void setMethodParamIndex(Map<String, Integer> methodParamIndex) {
        this.methodParamIndex = methodParamIndex;
    }

    public Object computeRouting(SpaceRemotingInvocation remotingEntry) {
        if (remotingEntry.getArguments() == null) {
            return null;
        }
        Integer paramIndex = methodParamIndex.get(remotingEntry.getMethodName());
        if (paramIndex == null) {
            paramIndex = defaultParamIndex;
        }

        if (remotingEntry.getArguments().length > paramIndex) {
            return remotingEntry.getArguments()[paramIndex].hashCode();
        }
        return null;
    }
}
