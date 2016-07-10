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

package com.gigaspaces.internal.cluster.node.impl.router.spacefinder;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.cluster.node.impl.router.LRMIAsyncContextProvider;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.core.client.SpaceURL;

/**
 * The default implementation of the {@link ISpaceProxyProvider}, uses space finder to locate a
 * proxy
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceFinderSpaceProxyProvider extends LRMIAsyncContextProvider implements ISpaceProxyProvider {
    @Override
    public RemoteSpaceResult getSpaceProxy(SpaceURL spaceURL) throws FinderException {
        IDirectSpaceProxy directProxy = (IDirectSpaceProxy) SpaceFinder.find(spaceURL);
        return new RemoteSpaceResult(directProxy.getRemoteJSpace(), directProxy.getRemoteMemberName());
    }
}
