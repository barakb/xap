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


package org.openspaces.core.space.filter;

import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.filters.FilterOperationCodes;
import com.j_spaces.core.filters.entry.ISpaceFilterEntry;

import net.jini.core.entry.UnusableEntryException;

import org.springframework.util.ClassUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>A filter operation delegate invoker, invoking a method associated with the given operation
 * code.
 *
 * <p>For single {@link com.j_spaces.core.filters.entry.ISpaceFilterEntry ISpaceFilterEntry}
 * invocation (see {@link com.j_spaces.core.filters.ISpaceFilter#process(com.j_spaces.core.SpaceContext,
 * com.j_spaces.core.filters.entry.ISpaceFilterEntry, int) process}) support the following different
 * structures: <ul> <li>A no op method callback. For example <code>test()</code></li> <li>A single
 * parameter. The parameter can either be an {@link com.j_spaces.core.filters.entry.ISpaceFilterEntry}
 * or the actual template object wrapped by the entry. Note, if using actual types, this delegate
 * will filter out all the types that are not assignable to it. For example:
 * <code>test(ISpaceFilterEntry entry)</li> or <code>test(SimpleMessage message)</code>. <li>Two
 * parameters. The first one maps to the previous option, the second one is the operation code.</li>
 * <li>Three parameters. The first two maps to the previous option, the third one is a {@link
 * com.j_spaces.core.SpaceContext}. </ul>
 *
 * <p>For multiple {@link com.j_spaces.core.filters.entry.ISpaceFilterEntry} invocation (see {@link
 * com.j_spaces.core.filters.ISpaceFilter#process(com.j_spaces.core.SpaceContext,
 * com.j_spaces.core.filters.entry.ISpaceFilterEntry[], int) process}) support the following
 * different structures: <ul> <li>A no op method callback. For example <code>test()</code></li>
 * <li>A single parameter. The parameter can either be an {@link com.j_spaces.core.filters.entry.ISpaceFilterEntry}
 * or the actual template object wrapped by the entry. Note, if using actual types, this delegate
 * will filter out all the types that are not assignable to it. For example:
 * <code>test(ISpaceFilterEntry entry)</li> or <code>test(SimpleMessage message)</code>. <li>Two
 * parameters. The first one maps to the previous option, the second is the same as the first one
 * since multiple entries always have two entries (mainly for update operations).</li> <li>Three
 * parameters. The first two maps to the previous option, the third one is the operation code.</li>
 * <li>Four parameters. The first three maps to the previous option, the fourth one is a {@link
 * com.j_spaces.core.SpaceContext}. </ul>
 *
 * @author kimchy
 */
class FilterOperationDelegateInvoker {

    private boolean filterOnTypes = true;

    private int operationCode;

    private IMethod processMethod;

    private Class<?>[] parameterTypes;

    private Map<String, Class> classCache = new ConcurrentHashMap<String, Class>();

    /**
     * Constructs a new delegate for the given operation code and a method to invoke.
     */
    public FilterOperationDelegateInvoker(int operationCode, Method processMethod) {
        this.operationCode = operationCode;
        this.processMethod = ReflectionUtil.createMethod(processMethod);
        this.parameterTypes = processMethod.getParameterTypes();
    }

    /**
     * Returns the operation code this delegate represents.
     */
    public int getOperationCode() {
        return operationCode;
    }

    /**
     * Returns the method that will be delegated to.
     */
    public IMethod getProcessMethod() {
        return processMethod;
    }

    /**
     * Invokes the method based on a single entry. See {@link FilterOperationDelegateInvoker}.
     */
    public void invokeProcess(IJSpace space, Object delegate, SpaceContext context, ISpaceFilterEntry entry)
            throws FilterExecutionException {
        Object[] params;
        if (parameterTypes.length == 0) {
            params = null;
        } else {
            if (operationCode == FilterOperationCodes.BEFORE_AUTHENTICATION) {
                params = new Object[]{context};
            } else {
                Object entryParam = entry;
                if (entryParam != null) {
                    entryParam = detectSpaceFilterEntryParam(parameterTypes[0], space, entry);
                    // perform filtering based on type
                    if (entryParam == null) {
                        return;
                    }
                }
                if (parameterTypes.length == 1) {
                    params = new Object[]{entryParam};
                } else if (parameterTypes.length == 2) {
                    params = new Object[]{entryParam, operationCode};
                } else if (parameterTypes.length == 3) {
                    params = new Object[]{entryParam, operationCode, context};
                } else {
                    throw new FilterExecutionException("Method [" + processMethod.getName() + "] should not have more than 3 parameters");
                }
            }
        }
        try {
            processMethod.invoke(delegate, params);
        } catch (IllegalAccessException e) {
            throw new FilterExecutionException("Failed to access method [" + processMethod.getName() +
                    "] with operation code [" + operationCode + "]", e);
        } catch (InvocationTargetException e) {
            throw new FilterExecutionException("Failed to execute method [" + processMethod.getName() +
                    "] with operation code [" + operationCode + "]", e.getTargetException());
        } catch (Exception e) {
            throw new FilterExecutionException("Failed to execute method [" + processMethod.getName() +
                    "] with operation code [" + operationCode + "]", e);
        }
    }

    /**
     * Invokes the method based on a multiple entries. See {@link FilterOperationDelegateInvoker}.
     */
    public void invokeProcess(IJSpace space, Object delegate, SpaceContext context, ISpaceFilterEntry[] entries)
            throws FilterExecutionException {
        Object[] params = null;
        if (parameterTypes.length == 0) {
            params = null;
        } else {
            Object entryParam1 = entries[0];
            if (entryParam1 != null) {
                entryParam1 = detectSpaceFilterEntryParam(parameterTypes[0], space, entries[0]);
                if (entryParam1 == null) {
                    return;
                }
            }
            if (parameterTypes.length == 1) {
                params = new Object[]{entryParam1};
            } else {
                Object entryParam2 = entries[1];
                if (entryParam2 != null) {
                    entryParam2 = detectSpaceFilterEntryParam(parameterTypes[1], space, entries[1]);
                    if (entryParam2 == null) {
                        return;
                    }
                }
                if (parameterTypes.length == 2) {
                    params = new Object[]{entryParam1, entryParam2};
                } else if (parameterTypes.length == 3) {
                    params = new Object[]{entryParam1, entryParam2, operationCode};
                } else if (parameterTypes.length == 4) {
                    params = new Object[]{entryParam1, entryParam2, operationCode, context};
                } else {
                    throw new FilterExecutionException("Method [" + processMethod.getName() + "] should not have more than 4 parameters");
                }
            }
        }
        try {
            processMethod.invoke(delegate, params);
        } catch (IllegalAccessException e) {
            throw new FilterExecutionException("Failed to access method [" + processMethod.getName() +
                    "] with operation code [" + operationCode + "]", e);
        } catch (InvocationTargetException e) {
            throw new FilterExecutionException("Failed to execute method [" + processMethod.getName() +
                    "] with operation code [" + operationCode + "]", e.getTargetException());
        } catch (Exception e) {
            throw new FilterExecutionException("Failed to execute method [" + processMethod.getName() +
                    "] with operation code [" + operationCode + "]", e);
        }
    }

    private Object detectSpaceFilterEntryParam(Class paramType, IJSpace space, ISpaceFilterEntry entry)
            throws FilterExecutionException {
        if (ISpaceFilterEntry.class.isAssignableFrom(paramType)) {
            return entry;
        }
        if (entry.getClassName() == null) {
            // in case of UID base operation, there is no classname, simply filter it out
            return null;
        }
        // TODO in the future, we might have classname with UID based API, in this case, we will still need to filter it
        if (filterOnTypes) {
            Class entryClass = classCache.get(entry.getClassName());
            if (entryClass == null) {
                try {
                    entryClass = ClassUtils.getDefaultClassLoader().loadClass(entry.getClassName());
                } catch (ClassNotFoundException e) {
                    throw new FilterExecutionException("Failed to find class [" + entry.getClassName() + "]", e);
                }
                classCache.put(entry.getClassName(), entryClass);
            }
            if (!paramType.isAssignableFrom(entryClass)) {
                return null;
            }
        }
        try {
            return entry.getObject(space);
        } catch (UnusableEntryException e) {
            throw new FilterExecutionException("Failed to get object from entry [" + entry + "]", e);
        }
    }
}
