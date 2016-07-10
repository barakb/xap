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

package com.gigaspaces.internal.reflection;

import java.lang.reflect.Method;

@com.gigaspaces.api.InternalApi
public class MethodHolder {

    final private Method _method;
    final private String _methodDescriptor;

    public MethodHolder(Method method) {
        _method = method;
        _methodDescriptor = ReflectionUtil.getMethodDescriptor(method);
    }

    public Method getMethod() {
        return _method;
    }

    public String getMethodDescriptor() {
        return _methodDescriptor;
    }

    public String getName() {
        return _method.getName();
    }

    @Override
    public int hashCode() {
        return getName().hashCode() ^ _methodDescriptor.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        MethodHolder otherMethod = (MethodHolder) other;
        return getName().equals(otherMethod.getName()) &&
                _methodDescriptor.equals(otherMethod.getMethodDescriptor());
    }
}
