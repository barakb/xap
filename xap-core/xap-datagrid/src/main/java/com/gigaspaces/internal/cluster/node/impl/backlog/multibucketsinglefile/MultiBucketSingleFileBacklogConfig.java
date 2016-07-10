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

package com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig;

@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileBacklogConfig
        extends BacklogConfig {

    private short _bucketsCount = 4;

    public void setBucketCount(short bucketsCount) {
        _bucketsCount = bucketsCount;
    }

    public short getBucketsCount() {
        return _bucketsCount;
    }

    @Override
    public MultiBucketSingleFileBacklogConfig clone() {
        MultiBucketSingleFileBacklogConfig clone = new MultiBucketSingleFileBacklogConfig();
        clone.overrideWithOther(this);
        clone.setBucketCount(getBucketsCount());
        return clone;
    }

    @Override
    public String toString() {
        return "MultiBucketSingleFileBacklogConfig [_bucketsCount="
                + _bucketsCount + ", " + super.toString() + "]";
    }


}
