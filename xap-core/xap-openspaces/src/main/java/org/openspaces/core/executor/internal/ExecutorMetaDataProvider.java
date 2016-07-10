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


package org.openspaces.core.executor.internal;

import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;

import org.openspaces.core.executor.TaskRoutingProvider;
import org.springframework.dao.DataAccessException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * A helper class allowing to extract meta data related to executors/tasks (such as routing
 * information).
 *
 * @author kimchy
 */
public class ExecutorMetaDataProvider {

    private Map<Class, IMethod> routingMethods = new CopyOnUpdateMap<Class, IMethod>();

    private static IMethod NO_METHOD;

    static {
        try {
            NO_METHOD = ReflectionUtil.createMethod(Object.class.getMethod("toString"));
        } catch (NoSuchMethodException e) {
            // won't happen
        }
    }

    public Object findRouting(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof TaskRoutingProvider) {
            return ((TaskRoutingProvider) obj).getRouting();
        }
        IMethod method = routingMethods.get(obj.getClass());
        if (method == null) {
            method = findRoutingMethod(obj);
            routingMethods.put(obj.getClass(), method);
        }
        if (method == NO_METHOD) {
            return null;
        }
        try {
            return method.invoke(obj);
        } catch (IllegalAccessException e) {
            throw new FailedToExecuteRoutingMethodException(e.getMessage(), e);
        } catch (InvocationTargetException e) {
            throw new FailedToExecuteRoutingMethodException(e.getTargetException().getMessage(), e.getTargetException());
        }
    }

    public static class FailedToExecuteRoutingMethodException extends DataAccessException {

        private static final long serialVersionUID = -3598757232489798078L;

        public FailedToExecuteRoutingMethodException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    private static IMethod findRoutingMethod(Object task) {
        Class targetClass = task.getClass();
        do {
            Method[] methods = targetClass.getDeclaredMethods();
            for (Method method : methods) {
                if (method.isAnnotationPresent(SpaceRouting.class)) {
                    method.setAccessible(true);
                    return ReflectionUtil.createMethod(method);
                }
            }
            targetClass = targetClass.getSuperclass();
        } while (targetClass != null);
        return NO_METHOD;
    }
}
