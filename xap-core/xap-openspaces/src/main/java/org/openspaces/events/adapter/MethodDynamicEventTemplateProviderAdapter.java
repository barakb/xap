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

package org.openspaces.events.adapter;

import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * The method event template provider adapter allows to configure the method name (using {@link
 * #setMethodName(String)} that the template request will be delegated to. The default method name
 * is <code>getDynamicTemplate</code>.
 *
 * @author Itai Frenkel
 * @since 9.1.1
 */
public class MethodDynamicEventTemplateProviderAdapter extends AbstractReflectionDynamicEventTemplateProviderAdapter {

    /**
     * Default method name to delegate to: <code>handleEvent</code>.
     */
    public static final String DEFAULT_LISTENER_METHOD_NAME = "getDynamicTemplate";

    private String methodName = DEFAULT_LISTENER_METHOD_NAME;

    /**
     * Sets the method name the event listener adapter will delegate the events to.
     *
     * @param methodName The method name events will be delegated to
     */
    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    /**
     * Returns a list of all the methods names that match the configured {@link
     * #setMethodName(String)}.
     */
    protected Method doGetListenerMethod() {
        final List<Method> methods = new ArrayList<Method>();
        ReflectionUtils.doWithMethods(getDelegate().getClass(), new ReflectionUtils.MethodCallback() {
            public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
                methods.add(method);
            }
        }, new ReflectionUtils.MethodFilter() {
            public boolean matches(Method method) {
                return method.getName().equals(methodName);
            }
        });

        Method listenerMethod = null;
        for (Method method : methods) {
            if (method.getParameterTypes().length > 0) {
                throw new IllegalArgumentException("Expected method " + methodName + " to have zero parameters");
            }
            if (listenerMethod != null) {
                throw new IllegalStateException("Expected method " + methodName + " to appear only once.");
            }
            listenerMethod = method;
        }
        return listenerMethod;
    }
}