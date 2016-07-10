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

import java.util.Set;

@com.gigaspaces.api.InternalApi
public class MirrorChannelReplicationSettings
        extends ReplicationSettings
        implements IMirrorChannelReplicationSettings {

    private final String _mirrorMemberName;

    public MirrorChannelReplicationSettings(String mirrorMemberName) {
        super();
        _mirrorMemberName = mirrorMemberName;
    }

    public String getMirrorMemberName() {
        return _mirrorMemberName;
    }

    @Override
    public boolean supportsChange() {
        return false;
    }

    @Override
    public Set<String> getSupportedChangeOperations() {
        return null;
    }

}
