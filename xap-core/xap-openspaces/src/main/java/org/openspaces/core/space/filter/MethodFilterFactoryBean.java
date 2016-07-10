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

import com.j_spaces.core.filters.FilterOperationCodes;

import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link com.j_spaces.core.filters.FilterProvider FilterProvider} factory that accepts a Pojo
 * filter with different operation callbacks that are marked using this factory. For each available
 * operation there is a setter that accepts the method name to be invoked if the operation happened.
 * Not setting a callback means that this filter will not listen to the mentioned operation. For
 * example, if the filter ({@link #setFilter(Object)}) has a method called
 * <code>doSomethingBeforeWrite</code>, the {@link #setBeforeWrite(String)} will need to be set with
 * <code>doSomethingBeforeWrite</code>.
 *
 * <p>The operation callback methods can different arguments. Please see {@link
 * org.openspaces.core.space.filter.FilterOperationDelegateInvoker} for all the different
 * possibilities.
 *
 * <p>For a Pojo adapter that uses annotation please see {@link AnnotationFilterFactoryBean}.
 *
 * @author kimchy
 * @see org.openspaces.core.space.filter.FilterOperationDelegate
 * @see com.j_spaces.core.filters.FilterProvider
 * @see com.j_spaces.core.filters.ISpaceFilter
 * @see com.j_spaces.core.filters.FilterOperationCodes
 */
public class MethodFilterFactoryBean extends AbstractFilterProviderAdapterFactoryBean {

    private String filterInit;

    private String filterClose;

    private String beforeAuthentication;

    private String beforeWrite;

    private String afterWrite;

    private String beforeRead;

    private String afterRead;

    private String beforeTake;

    private String afterTake;

    private String beforeNotify;

    private String beforeCleanSpace;

    private String beforeUpdate;

    private String afterUpdate;

    private String beforeReadMultiple;

    private String afterReadMultiple;

    private String beforeTakeMultiple;

    private String afterTakeMultiple;

    private String beforeNotifyTrigger;

    private String afterNotifyTrigger;

    private String beforeAllNotifyTrigger;

    private String afterAllNotifyTrigger;

    private String beforeRemoveByLease;

    private String afterRemoveByLease;

    private String beforeExecute;

    private String afterExecute;

    private String beforeChange;

    private String afterChange;

    /**
     * Creates an operation code to filter invoker map based on the {@link #getFilter()} delegate
     * and the callbacks set on this factory.
     */
    @Override
    protected Map<Integer, FilterOperationDelegateInvoker> doGetInvokerLookup() {
        final Map<Integer, FilterOperationDelegateInvoker> invokerLookup = new HashMap<Integer, FilterOperationDelegateInvoker>();
        ReflectionUtils.doWithMethods(getFilter().getClass(), new ReflectionUtils.MethodCallback() {
            @Override
            public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeAuthentication)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_AUTHENTICATION);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeWrite)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_WRITE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), afterWrite)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.AFTER_WRITE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeRead)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_READ);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), afterRead)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.AFTER_READ);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeTake)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_TAKE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), afterTake)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.AFTER_TAKE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeNotify)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_NOTIFY);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeCleanSpace)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_CLEAN_SPACE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeUpdate)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_UPDATE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), afterUpdate)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.AFTER_UPDATE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeReadMultiple)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_READ_MULTIPLE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), afterReadMultiple)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.AFTER_READ_MULTIPLE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeTakeMultiple)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_TAKE_MULTIPLE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), afterTakeMultiple)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.AFTER_TAKE_MULTIPLE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeNotifyTrigger)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_NOTIFY_TRIGGER);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), afterNotifyTrigger)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.AFTER_NOTIFY_TRIGGER);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeAllNotifyTrigger)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_ALL_NOTIFY_TRIGGER);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), afterAllNotifyTrigger)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.AFTER_ALL_NOTIFY_TRIGGER);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeRemoveByLease)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_REMOVE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), afterRemoveByLease)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.AFTER_REMOVE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeExecute)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_EXECUTE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), afterExecute)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.AFTER_EXECUTE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeChange)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.BEFORE_CHANGE);
                }
                if (ObjectUtils.nullSafeEquals(method.getName(), beforeChange)) {
                    addInvoker(invokerLookup, method, FilterOperationCodes.AFTER_CHANGE);
                }
            }
        }, new UniqueMethodFilter());
        return invokerLookup;
    }

    /**
     * Returns the filter lifecycle method set with {@link #setFilterInit(String)}.
     */
    @Override
    protected Method doGetInitMethod() {
        final AtomicReference<Method> ref = new AtomicReference<Method>();
        ReflectionUtils.doWithMethods(getFilter().getClass(), new ReflectionUtils.MethodCallback() {
            @Override
            public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
                if (ObjectUtils.nullSafeEquals(method.getName(), filterInit)) {
                    ref.set(method);
                }
            }
        }, new UniqueMethodFilter());
        return ref.get();
    }

    /**
     * Returns the filter lifecycle method set with {@link #setFilterClose(String)}.
     */
    @Override
    protected Method doGetCloseMethod() {
        final AtomicReference<Method> ref = new AtomicReference<Method>();
        ReflectionUtils.doWithMethods(getFilter().getClass(), new ReflectionUtils.MethodCallback() {
            @Override
            public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
                if (ObjectUtils.nullSafeEquals(method.getName(), filterClose)) {
                    ref.set(method);
                }
            }
        }, new UniqueMethodFilter());
        return ref.get();
    }

    /**
     * Method name for filter lifecycle init callback. Can either have no arguments or a single
     * argument that accepts {@link com.j_spaces.core.IJSpace}.
     */
    public void setFilterInit(String filterInit) {
        this.filterInit = filterInit;
    }

    /**
     * Method name for filter lifecycle close callback. Should have no arguments.
     */
    public void setFilterClose(String filterClose) {
        this.filterClose = filterClose;
    }

    /**
     * Filter callback before write operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_WRITE
     */
    public void setBeforeWrite(String beforeWrite) {
        this.beforeWrite = beforeWrite;
    }

    /**
     * Filter callback before authentication operation. Note, the method must have a single
     * parameter of type {@link com.j_spaces.core.SpaceContext}.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_AUTHENTICATION
     */
    public void setBeforeAuthentication(String beforeAuthentication) {
        this.beforeAuthentication = beforeAuthentication;
    }

    /**
     * Filter callback after write operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#AFTER_WRITE
     */
    public void setAfterWrite(String afterWrite) {
        this.afterWrite = afterWrite;
    }

    /**
     * Filter callback before read operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_READ
     */
    public void setBeforeRead(String beforeRead) {
        this.beforeRead = beforeRead;
    }

    /**
     * Filter callback after read operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#AFTER_READ
     */
    public void setAfterRead(String afterRead) {
        this.afterRead = afterRead;
    }

    /**
     * Filter callback before take operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_TAKE
     */
    public void setBeforeTake(String beforeTake) {
        this.beforeTake = beforeTake;
    }

    /**
     * Filter callback after take operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#AFTER_TAKE
     */
    public void setAfterTake(String afterTake) {
        this.afterTake = afterTake;
    }

    /**
     * Filter callback before notify operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_NOTIFY
     */
    public void setBeforeNotify(String beforeNotify) {
        this.beforeNotify = beforeNotify;
    }

    /**
     * Filter callback after clean space operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_CLEAN_SPACE
     */
    public void setBeforeCleanSpace(String beforeCleanSpace) {
        this.beforeCleanSpace = beforeCleanSpace;
    }

    /**
     * Filter callback before update operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_UPDATE
     */
    public void setBeforeUpdate(String beforeUpdate) {
        this.beforeUpdate = beforeUpdate;
    }

    /**
     * Filter callback after update operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#AFTER_UPDATE
     */
    public void setAfterUpdate(String afterUpdate) {
        this.afterUpdate = afterUpdate;
    }

    /**
     * Filter callback before read multiple operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_READ_MULTIPLE
     */
    public void setBeforeReadMultiple(String beforeReadMultiple) {
        this.beforeReadMultiple = beforeReadMultiple;
    }

    /**
     * Filter callback after read multiple operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#AFTER_READ_MULTIPLE
     */
    public void setAfterReadMultiple(String afterReadMultiple) {
        this.afterReadMultiple = afterReadMultiple;
    }

    /**
     * Filter callback before take multiple operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_TAKE_MULTIPLE
     */
    public void setBeforeTakeMultiple(String beforeTakeMultiple) {
        this.beforeTakeMultiple = beforeTakeMultiple;
    }

    /**
     * Filter callback after take multiple operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#AFTER_TAKE_MULTIPLE
     */
    public void setAfterTakeMultiple(String afterTakeMultiple) {
        this.afterTakeMultiple = afterTakeMultiple;
    }

    /**
     * Filter callback before notify trigger. Indicates that a matched notify template was found to
     * the current entry event.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_NOTIFY_TRIGGER
     */
    public void setBeforeNotifyTrigger(String beforeNotifyTrigger) {
        this.beforeNotifyTrigger = beforeNotifyTrigger;
    }

    /**
     * Filter callback after notify trigger. Indicates that a notify trigger was successful.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#AFTER_NOTIFY_TRIGGER
     */
    public void setAfterNotifyTrigger(String afterNotifyTrigger) {
        this.afterNotifyTrigger = afterNotifyTrigger;
    }

    /**
     * Filter callback before all notify trigger. Indicates that at least one notify template is
     * matched to the current entry event.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_ALL_NOTIFY_TRIGGER
     */
    public void setBeforeAllNotifyTrigger(String beforeAllNotifyTrigger) {
        this.beforeAllNotifyTrigger = beforeAllNotifyTrigger;
    }

    /**
     * Filter callback after all notify trigger. Indicates that all notify templates that are
     * matched to the current entry event were triggered and returned or failed.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#AFTER_ALL_NOTIFY_TRIGGER
     */
    public void setAfterAllNotifyTrigger(String afterAllNotifyTrigger) {
        this.afterAllNotifyTrigger = afterAllNotifyTrigger;
    }

    /**
     * Filter callback before an entry was removed due to lease expression or lease cancel.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_REMOVE
     */
    public void setBeforeRemoveByLease(String beforeRemoveByLease) {
        this.beforeRemoveByLease = beforeRemoveByLease;
    }

    /**
     * Filter callback after an entry was removed due to lease expression or lease cancel.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#AFTER_REMOVE
     */
    public void setAfterRemoveByLease(String afterRemoveByLease) {
        this.afterRemoveByLease = afterRemoveByLease;
    }

    /**
     * Filter callback before execute operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_EXECUTE
     */
    public void setBeforeExecute(String beforeExecute) {
        this.beforeExecute = beforeExecute;
    }

    /**
     * Filter callback after execute operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#AFTER_EXECUTE
     */
    public void setAfterExecute(String afterExecute) {
        this.afterExecute = afterExecute;
    }

    /**
     * Filter callback before change operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#BEFORE_CHANGE
     */
    public void setBeforeChange(String beforeChange) {
        this.beforeChange = beforeChange;
    }

    /**
     * Filter callback after change operation.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes#AFTER_CHANGE
     */
    public void setAfterChange(String afterChange) {
        this.afterChange = afterChange;
    }

    private static class UniqueMethodFilter implements ReflectionUtils.MethodFilter {

        private Set<String> processedMethods = new HashSet<String>();

        @Override
        public boolean matches(Method method) {
            if (processedMethods.contains(method.getName())) {
                return false;
            }
            processedMethods.add(method.getName());
            return true;
        }
    }
}
