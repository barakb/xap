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

package com.gigaspaces.internal.cluster.node.impl.config;


@com.gigaspaces.api.InternalApi
public class ReliableAsyncReplicationSettings
        implements IReliableAsyncReplicationSettings {
    private final ISpaceReplicationSettings _spaceSettings;
    private final IMirrorChannelReplicationSettings _mirrorSettings;
    private final boolean _keeperCannotRefillMissingContent;
    private final long _reliableAsyncCompletionNotifierInterval;
    private final long _reliableAsyncCompletionNotifierPacketsThreshold;

    public ReliableAsyncReplicationSettings(
            ISpaceReplicationSettings spaceSettings,
            IMirrorChannelReplicationSettings mirrorSettings, boolean keeperCannotRefillMissingContent, long reliableAsyncCompletionNotifierInterval, long reliableAsyncCompletionNotifierPacketsThreshold) {
        super();
        _spaceSettings = spaceSettings;
        _mirrorSettings = mirrorSettings;
        _keeperCannotRefillMissingContent = keeperCannotRefillMissingContent;
        _reliableAsyncCompletionNotifierInterval = reliableAsyncCompletionNotifierInterval;
        _reliableAsyncCompletionNotifierPacketsThreshold = reliableAsyncCompletionNotifierPacketsThreshold;
    }

    public ISpaceReplicationSettings getSpaceReplicationSettings() {
        return _spaceSettings;
    }

    public IMirrorChannelReplicationSettings getMirrorReplicationSettings() {
        return _mirrorSettings;
    }

    @Override
    public boolean keeperCannotRefillMissingContent() {
        return _keeperCannotRefillMissingContent;
    }

    @Override
    public long getReliableAsyncCompletionNotifierInterval() {
        return _reliableAsyncCompletionNotifierInterval;
    }

    @Override
    public long getReliableAsyncCompletionNotifierPacketsThreshold() {
        return _reliableAsyncCompletionNotifierPacketsThreshold;
    }

}
