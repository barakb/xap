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

package com.gigaspaces.internal.cluster.node.impl.groups.sync;

@com.gigaspaces.api.InternalApi
public class BacklogAdjustedThrottleControllerBuilder
        implements IReplicationThrottleControllerBuilder {

    private final int _maxTPWhenInactive;
    private final int _minTPWhenActive;
    private final int _threshold;
    private final boolean _throttleWhenInactive;

    public BacklogAdjustedThrottleControllerBuilder(int maxTPWhenInactive,
                                                    int minTPWhenActive, int threshold, boolean throttleWhenInactive) {
        _maxTPWhenInactive = maxTPWhenInactive;
        _minTPWhenActive = minTPWhenActive;
        _threshold = threshold;
        _throttleWhenInactive = throttleWhenInactive;
    }

    public IReplicationThrottleController createController(String groupName,
                                                           String sourceMemberName, String targetMemberName) {
        return new BacklogAdjustedThrottleController(_maxTPWhenInactive,
                _minTPWhenActive,
                _threshold,
                200,
                20,
                groupName,
                sourceMemberName,
                targetMemberName,
                _throttleWhenInactive);
    }

    @Override
    public String toString() {
        return "BacklogAdjustedThrottleControllerBuilder [maxTPWhenInactive="
                + _maxTPWhenInactive + ", minTPWhenActive=" + _minTPWhenActive
                + ", threshold=" + _threshold + "]";
    }

}
