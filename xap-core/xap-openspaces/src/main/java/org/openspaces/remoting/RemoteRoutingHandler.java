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

/**
 * A general interface allowing for pluggable computation of the remoting invocation routing field.
 * Routing field controls the partition the invocation will be directed to when working with a
 * partitioned space.
 *
 * @author kimchy
 */
public interface RemoteRoutingHandler<T> {

    /**
     * Returns the routing field value based on the remoting invocation. If <code>null</code> is
     * returned, will use internal calcualtion of the routing index.
     */
    T computeRouting(SpaceRemotingInvocation remotingEntry);
}
