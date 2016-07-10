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

import org.springframework.aop.support.AopUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * An event listener adapter that uses {@link SpaceDataEvent} annotation in order to find event
 * listener methods to delegate to.
 *
 * @author kimchy
 */
public class AnnotationEventListenerAdapter extends AbstractReflectionEventListenerAdapter {

    /**
     * Goes over all the methods in the delegate and adds them as event listeners if they have
     * {@link SpaceDataEvent} annotation.
     */
    protected Method[] doGetListenerMethods() {
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
                return method.getAnnotation(SpaceDataEvent.class) != null;
            }
        });
        return methods.toArray(new Method[methods.size()]);
    }
}
