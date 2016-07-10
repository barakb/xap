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
import org.openspaces.events.DynamicEventTemplateProvider;
import org.openspaces.events.ListenerExecutionFailedException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.util.Assert;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Base class for reflection driven invocation of event template provider method. The event listener
 * method os found and delegated to an object configured using {@link #setDelegate(Object)}.
 *
 * <p> Subclasses must implement the {@link #doGetListenerMethod()} in order to list the possible
 * event listener method.
 *
 * @author Itai Frenkel
 * @since 9.1.1
 */
public abstract class AbstractReflectionDynamicEventTemplateProviderAdapter implements
        InitializingBean, DynamicEventTemplateProvider {

    /**
     * Logger available to subclasses
     */
    protected final Log logger = LogFactory.getLog(getClass());

    private Object delegate;

    private Method listenerMethod;

    private IMethod fastMethod;

    private boolean useFastRefelction = true;

    private boolean failSilentlyIfAnnotationNotFound;

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
     * Controls if the listener will be invoked using fast reflection or not. Defaults to
     * <code>true</code>.
     */
    public void setUseFastReflection(boolean useFastReflection) {
        this.useFastRefelction = useFastReflection;
    }

    public void afterPropertiesSet() {
        Assert.notNull(delegate, "delegate property is required");
        listenerMethod = doGetListenerMethod();
        if (listenerMethod != null) {
            if (useFastRefelction) {
                fastMethod = ReflectionUtil.createMethod(listenerMethod);
            } else {
                fastMethod = new StandardMethod(listenerMethod);
            }

            listenerMethod.setAccessible(true);
        } else if (!failSilentlyIfAnnotationNotFound) {
            throw new IllegalArgumentException("No event template provider method found in delegate [" + delegate + "]");
        }
    }

    /**
     * Delegates the event listener invocation to the appropriate method of the configured {@link
     * #setDelegate(Object)}.
     */
    @Override
    public Object getDynamicTemplate() {

        Object result = null;

        // single method, use the already obtained Method to invoke the listener
        try {
            result = fastMethod.invoke(delegate);
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

        return result;
    }

    /**
     * @param failSilently - when set to true {@link #afterPropertiesSet()} will not raise an
     *                     exception if an annotated method is not found.
     */
    public void setFailSilentlyIfMethodNotFound(boolean failSilently) {
        this.failSilentlyIfAnnotationNotFound = failSilently;
    }

    public boolean isMethodFound() {
        return fastMethod != null;
    }

    /**
     * Subclasses should implement this in order to provide a list of all the possible event
     * listener delegate methods.
     *
     * @return A list of all the event listener delegate methods
     */
    protected abstract Method doGetListenerMethod();
}
