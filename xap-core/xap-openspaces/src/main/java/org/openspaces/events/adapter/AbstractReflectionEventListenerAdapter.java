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

import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.reflection.standard.StandardMethod;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.GigaSpace;
import org.openspaces.events.ListenerExecutionFailedException;
import org.openspaces.events.SpaceDataEventListener;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.util.Assert;
import org.springframework.util.MethodInvoker;
import org.springframework.util.ObjectUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Base class for reflection driven invocation of event listener methods. Handles cases where a
 * single event listener method is configured, or several method event listeners are configured (all
 * must have the same name). The event listener methods are found and delegated to an object
 * configured using {@link #setDelegate(Object)}.
 *
 * <p> Subclasses must implement the {@link #doGetListenerMethods()} in order to list all the
 * possible event listener methods. If more than one event listener method is found, all event
 * listener methods must have the same name.
 *
 * <p> For best performance a single event listener method should be used. If a single event
 * listener is found (by subclasses), caching of the method can be done in order to perform the
 * reflection execution faster. If more than one event listener method is found - dynamic discovery
 * of the appropriate method is done for each listener invocation.
 *
 * <p> Event listening methods can have no parameters or one or more parameters mapping to {@link
 * SpaceDataEventListener#onEvent(Object, org.openspaces.core.GigaSpace,
 * org.springframework.transaction.TransactionStatus, Object)} parameters order. If the method has a
 * return value it will be handled thanks to {@link org.openspaces.events.adapter.AbstractResultEventListenerAdapter}.
 *
 * <p> Having more than one event listening method allows for writing specific listener methods
 * handling different data event types (usually different types within the same inheritance tree).
 * This allows to remove the need for <code>instanceof</code> checks within the listener code. If a
 * single listening method is used, it can still have a specific class type for the event data
 * object thanks to Java reflection, though if the event listener will be invoked with a different
 * type a reflection exception will be thrown.
 *
 * @author kimchy
 */
public abstract class AbstractReflectionEventListenerAdapter extends AbstractResultEventListenerAdapter implements
        InitializingBean, EventListenerAdapter {

    /**
     * Logger available to subclasses
     */
    protected final Log logger = LogFactory.getLog(getClass());

    private Object delegate;

    private Method[] listenerMethods;

    private IMethod fastMethod;

    private int numberOfParameters;

    private boolean useFastReflection = true;

    //private boolean notifyPreviousValueOnUpdate = false;

    /**
     * The event listener delegate that will be searched for event listening methods and will have
     * its method executed.
     */
    public void setDelegate(Object delegate) {
        this.delegate = delegate;
    }

    /**
     * Returns the event listener delegate.
     */
    protected Object getDelegate() {
        return this.delegate;
    }

    /**
     * Returns the flag that indicates whether the listener method(s) should get the old and new values (on event notifications).
     */
    /*
    protected boolean isNotifyPreviousValueOnUpdate() {
        return notifyPreviousValueOnUpdate;
    }
    */

    /**
     * The flag that indicates whether the listener method(s) should get the old and new values (from event notifications).
     */
    /*
    public void setNotifyPreviousValueOnUpdate(boolean notifyPreviousValueOnUpdate) {
        this.notifyPreviousValueOnUpdate = notifyPreviousValueOnUpdate;
    }
    */

    /**
     * Controls if the listener will be invoked using fast reflection or not. Defaults to
     * <code>true</code>.
     */
    public void setUseFastReflection(boolean useFastReflection) {
        this.useFastReflection = useFastReflection;
    }

    public void afterPropertiesSet() {
        Assert.notNull(delegate, "delegate property is required");
        listenerMethods = doGetListenerMethods();
        if (listenerMethods == null || listenerMethods.length == 0) {
            throw new IllegalArgumentException("No event listening methods found in delegate [" + delegate + "]");
        }
        if (listenerMethods.length > 1) {
            // more than one listener methods are allowed, but they must be with the same name
            // if more than one listener method is defined, execution will be slower
            String methodName = listenerMethods[0].getName();
            for (int i = 1; i < listenerMethods.length; i++) {
                if (!methodName.equals(listenerMethods[i].getName())) {
                    throw new IllegalArgumentException("All listener methods must have the same name. Found ["
                            + methodName + "] and [" + listenerMethods[i].getName() + "]");
                }
            }
            int numbersOfParams = listenerMethods[0].getParameterTypes().length;
            for (int i = 1; i < listenerMethods.length; i++) {
                if (numbersOfParams != listenerMethods[i].getParameterTypes().length) {
                    throw new IllegalArgumentException("All listener methods must have number of parameters. Found ["
                            + numbersOfParams + "] and [" + listenerMethods[i].getParameterTypes().length + "]");
                }
            }
        } else {
            if (useFastReflection) {
                fastMethod = ReflectionUtil.createMethod(listenerMethods[0]);
            } else {
                fastMethod = new StandardMethod(listenerMethods[0]);
            }
        }
        for (Method listenerMethod : listenerMethods) {
            listenerMethod.setAccessible(true);
        }
        this.numberOfParameters = listenerMethods[0].getParameterTypes().length;
    }

    /**
     * Delegates the event listener invocation to the appropriate method of the configured {@link
     * #setDelegate(Object)}. If a single event listener delegate method is found, uses the cached
     * reflection Method. If more than one event listener delegate method is configured uses
     * reflection to dynamically find the relevant event listener method.
     */
    @Override
    protected Object onEventWithResult(Object data, GigaSpace gigaSpace, TransactionStatus txStatus, Object source) {
        Method listenerMethod = listenerMethods[0];

        /*
        Object newValue = null, oldValue = null;
        boolean doubleParameter = false;
        if (notifyPreviousValueOnUpdate && source instanceof EntryArrivedRemoteEvent) {
            EntryArrivedRemoteEvent event = (EntryArrivedRemoteEvent) source;
            try {
                newValue = event.getNewObject();
                oldValue = event.getOldObject();
                doubleParameter = true;
            } catch (Exception e) {
                newValue = null;
                oldValue = null;
            }
        }

        // set up the arguments passed to the method
        Object[] listenerArguments = null;
        if (numberOfParameters == 1) {
            listenerArguments = new Object[] { data };
        } else if (numberOfParameters == 2) {
            if (doubleParameter) {
                listenerArguments = new Object[] { oldValue, newValue };
            } else {
                listenerArguments = new Object[]{ data, gigaSpace};
            }
        } else if (numberOfParameters == 3) {
            if (doubleParameter) {
                listenerArguments = new Object[] { oldValue, newValue, gigaSpace };
            } else {
                listenerArguments = new Object[]{ data, gigaSpace, txStatus};
            }
        } else if (numberOfParameters == 4) {
            if (doubleParameter) {
                listenerArguments = new Object[] { oldValue, newValue, gigaSpace, txStatus };
            } else {
                listenerArguments = new Object[]{ data, gigaSpace, txStatus, source};
            }
        } else if (numberOfParameters == 5 && doubleParameter) {
            listenerArguments = new Object[] { oldValue, newValue, gigaSpace, txStatus, source};
        }
        */
        // set up the arguments passed to the method
        Object[] listenerArguments = null;
        if (numberOfParameters == 1) {
            listenerArguments = new Object[]{data};
        } else if (numberOfParameters == 2) {
            listenerArguments = new Object[]{data, gigaSpace};
        } else if (numberOfParameters == 3) {
            listenerArguments = new Object[]{data, gigaSpace, txStatus};
        } else if (numberOfParameters == 4) {
            listenerArguments = new Object[]{data, gigaSpace, txStatus, source};
        }

        Object result = null;
        if (listenerMethods.length == 1) {
            // single method, use the already obtained Method to invoke the listener
            try {
                result = fastMethod.invoke(delegate, listenerArguments);
            } catch (IllegalAccessException ex) {
                throw new PermissionDeniedDataAccessException("Failed to invoke event method ["
                        + listenerMethod.getName() + "]", ex);
            } catch (InvocationTargetException ex) {
                throw new ListenerExecutionFailedException("Listener event method [" + listenerMethod.getName()
                        + "] of class [" + listenerMethod.getDeclaringClass().getName() + "] threw exception", ex.getTargetException());
            } catch (Throwable ex) {
                throw new ListenerExecutionFailedException("Listener event method [" + listenerMethod.getName()
                        + "] of class [" + listenerMethod.getDeclaringClass().getName() + "] threw exception", ex);
            }
        } else {
            // more than one method, we need to use reflection to find the matched method
            MethodInvoker methodInvoker = new MethodInvoker();
            methodInvoker.setTargetObject(getDelegate());
            methodInvoker.setTargetMethod(listenerMethod.getName());
            methodInvoker.setArguments(listenerArguments);
            try {
                methodInvoker.prepare();
                result = methodInvoker.invoke();
            } catch (InvocationTargetException ex) {
                throw new ListenerExecutionFailedException("Listener method '"
                        + listenerMethod.getName() + "' threw exception", ex.getTargetException());
            } catch (Throwable ex) {
                throw new ListenerExecutionFailedException("Failed to invoke target method '"
                        + listenerMethod.getName() + "' with arguments "
                        + ObjectUtils.nullSafeToString(listenerArguments), ex);
            }

        }
        return result;
    }

    public Object getActualEventListener() {
        return this.delegate;
    }

    /**
     * Subclasses should implement this in order to provide a list of all the possible event
     * listener delegate methods.
     *
     * @return A list of all the event listener delegate methods
     */
    protected abstract Method[] doGetListenerMethods();
}
