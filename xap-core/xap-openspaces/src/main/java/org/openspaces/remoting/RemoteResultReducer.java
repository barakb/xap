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
 * A remoting result reducer (ala Map Reduce) used when working with {@link
 * org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean} in broadcast mode in order to
 * reduce the broadcast results into a "client side" result value.
 *
 * @author kimchy
 */
public interface RemoteResultReducer<T, Y> {

    /**
     * Reduces a list of Space remoting invocation results to an Object value. Can use the provided
     * remote invocation to perform different reduce operations, for example based on the {@link
     * SpaceRemotingInvocation#getMethodName()}.
     *
     * <p>An exception thrown from the reduce operation will be propagated to the client.
     *
     * @param results            A list of results from (usually) broadcast sync remote invocation.
     * @param remotingInvocation The remote invocation
     * @return A reduced return value (to the calling client)
     * @throws Exception An exception that will be propagated to the client
     */
    T reduce(SpaceRemotingResult<Y>[] results, SpaceRemotingInvocation remotingInvocation) throws Exception;
}
