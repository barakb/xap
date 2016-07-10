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

import org.openspaces.events.DynamicEventTemplate;
import org.springframework.aop.support.AopUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


/**
 * The annotation event template provider adapter uses {@link DynamicEventTemplate} annotation in
 * order to find the event template provider to delegate to.
 *
 * @author Itai Frenkel
 * @since 9.1.1
 */
public class AnnotationDynamicEventTemplateProviderAdapter extends AbstractReflectionDynamicEventTemplateProviderAdapter {

    /**
     * Goes over all the methods in the delegate and adds them as event listeners if they have
     * {@link DynamicEventTemplate} annotation.
     */
    protected Method doGetListenerMethod() {
        final List<Method> methods = new ArrayList<Method>();
        if (logger.isDebugEnabled()) {
            logger.debug("Thread.currentThread().getContextClassLoader()=" + Thread.currentThread().getContextClassLoader());
        }
        ReflectionUtils.doWithMethods(AopUtils.getTargetClass(getDelegate()), new ReflectionUtils.MethodCallback() {
            public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
                if (AopUtils.getTargetClass(getDelegate()) != getDelegate().getClass()) {
                    Method proxyMethod = null;
                    try {
                        proxyMethod = getDelegate().getClass().getMethod(method.getName(), method.getParameterTypes());
                        methods.add(proxyMethod);
                    } catch (NoSuchMethodException e) {
                        throw new IllegalArgumentException("Failed to find method [" + method + "] on proxy class [" + getDelegate().getClass().getName() + "]");
                    }
                } else {
                    methods.add(method);
                }
            }
        }, new ReflectionUtils.MethodFilter() {
            public boolean matches(Method method) {
                return method.getAnnotation(DynamicEventTemplate.class) != null;
            }
        });

        Method listenerMethod = null;
        for (Method method : methods) {
            if (method.getParameterTypes().length > 0) {
                throw new IllegalArgumentException("Expected annotated @" + DynamicEventTemplate.class.getName() + " method to have zero parameters");
            }
            if (listenerMethod != null) {
                throw new IllegalStateException("Expected annotated @" + DynamicEventTemplate.class.getName() + " method to appear only once.");
            }
            listenerMethod = method;
        }
        return listenerMethod;
    }
}
