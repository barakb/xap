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

import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeContext;
import com.gigaspaces.internal.utils.StringUtils;

@com.gigaspaces.api.InternalApi
public class MultiStepHandshakeContext
        implements IHandshakeContext {

    private final IHandshakeContext[] _steps;
    private int _index = 0;

    public MultiStepHandshakeContext(IHandshakeContext... steps) {
        _steps = steps;
    }

    @Override
    public boolean isDone() {
        while (_index < _steps.length) {
            if (_steps[_index].isDone())
                _index++;
            return false;
        }
        return true;
    }

    public IHandshakeContext getCurrentStep() {
        return _steps[Math.min(_steps.length - 1, _index)];
    }

    @Override
    public String toLogMessage() {
        StringBuilder builder = new StringBuilder("Multi step handshake context:");
        builder.append(StringUtils.NEW_LINE);
        for (IHandshakeContext step : _steps) {
            builder.append(step.toLogMessage());
            builder.append(StringUtils.NEW_LINE);
        }
        return builder.toString();
    }

}
