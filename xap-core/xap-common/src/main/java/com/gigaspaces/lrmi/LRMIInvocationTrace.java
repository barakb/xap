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

package com.gigaspaces.lrmi;

import java.util.Arrays;

/**
 * Represents a trace of an lrmi invocation, this object is immutable and when updated a new copy is
 * created and should be used instead of the current instance.
 *
 * @author eitany
 * @since 7.5
 */
@com.gigaspaces.api.InternalApi
public class LRMIInvocationTrace {
    private String _method;
    private Object[] _args;
    private String _identifier;
    private final boolean _isOutgoing;

    public LRMIInvocationTrace(String method, Object[] args, String identifier, boolean isOutgoing) {
        _method = method;
        _args = args;
        _identifier = identifier;
        _isOutgoing = isOutgoing;
    }

    public String getMethod() {
        return _method;
    }

    public LRMIInvocationTrace setMethod(String method) {
        return new LRMIInvocationTrace(method, _args, _identifier, _isOutgoing);
    }

    public Object[] getArgs() {
        return _args;
    }

    public LRMIInvocationTrace setArgs(Object[] args) {
        return new LRMIInvocationTrace(_method, args, _identifier, _isOutgoing);
    }

    public String getIdentifier() {
        return _identifier;
    }

    public LRMIInvocationTrace setIdentifier(String identifier) {
        return new LRMIInvocationTrace(_method, _args, identifier, _isOutgoing);
    }

    public boolean isOutgoing() {
        return _isOutgoing;
    }

    public String getTraceShortDisplayString() {
        if (_method == null)
            return "null method";
        return _method + " [" + _identifier + ", " + (_isOutgoing ? "outgoing" : "incoming") + "]";
    }

    public String getTraceLongDisplayString() {
        if (_method == null)
            return "null method";
        return _method + "(" + (_args == null ? "null" : Arrays.toString(_args)) + ") [" + _identifier + ", " + (_isOutgoing ? "outgoing" : "incoming") + "]";
    }
}