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


package org.openspaces.events.asyncpolling;

import org.openspaces.core.GigaSpace;
import org.openspaces.events.DynamicEventTemplateProvider;
import org.openspaces.events.EventExceptionHandler;
import org.openspaces.events.SpaceDataEventListener;
import org.openspaces.events.adapter.AnnotationDynamicEventTemplateProviderAdapter;
import org.openspaces.events.adapter.AnnotationEventListenerAdapter;
import org.openspaces.events.adapter.MethodDynamicEventTemplateProviderAdapter;
import org.openspaces.events.adapter.MethodEventListenerAdapter;
import org.openspaces.events.asyncpolling.receive.AsyncOperationHandler;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * A simplified programmatic configuration that for {@link SimpleAsyncPollingEventListenerContainer}.
 *
 * <p>Sample usage:
 * <pre>
 * UrlSpaceConfigurer urlSpaceConfigurerPrimary = new UrlSpaceConfigurer("/./space");
 * GigaSpace gigaSpace = new GigaSpaceConfigurer(urlSpaceConfigurerPrimary.space()).gigaSpace();
 * SimpleAsyncPollingEventListenerContainer pollingEventListenerContainer = new
 * SimpleAsyncPollingContainerConfigurer(gigaSpace)
 *              .template(new TestMessage())
 *              .eventListenerAnnotation(new Object() {
 *                  <code>@SpaceDataEvent</code> public void gotMeselfAnEvent() {
 *                      // do something
 *                  }
 *              }).asyncPollingContainer();
 *
 * ...
 *
 * pollingEventListenerContainer.destroy();
 * urlSpaceConfigurerPrimary.destroy();
 * </pre>
 *
 * @author kimchy
 * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer
 */
public class SimpleAsyncPollingContainerConfigurer {

    private final SimpleAsyncPollingEventListenerContainer pollingEventListenerContainer;

    private boolean initialized = false;

    public SimpleAsyncPollingContainerConfigurer(GigaSpace gigaSpace) {
        pollingEventListenerContainer = new SimpleAsyncPollingEventListenerContainer();
        pollingEventListenerContainer.setGigaSpace(gigaSpace);
    }

    /**
     * @see org.openspaces.events.AbstractEventListenerContainer#setBeanName(String)
     */
    public SimpleAsyncPollingContainerConfigurer name(String name) {
        pollingEventListenerContainer.setBeanName(name);
        return this;
    }

    /**
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setConcurrentConsumers(int)
     */
    public SimpleAsyncPollingContainerConfigurer concurrentConsumers(int concurrentConsumers) {
        pollingEventListenerContainer.setConcurrentConsumers(concurrentConsumers);
        return this;
    }

    /**
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setReceiveTimeout(long)
     */
    public SimpleAsyncPollingContainerConfigurer receiveTimeout(long receiveTimeout) {
        pollingEventListenerContainer.setReceiveTimeout(receiveTimeout);
        return this;
    }

    public SimpleAsyncPollingContainerConfigurer asyncOperationHandler(AsyncOperationHandler operationHandler) {
        pollingEventListenerContainer.setAsyncOperationHandler(operationHandler);
        return this;
    }

    /**
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setTemplate(Object)
     */
    public SimpleAsyncPollingContainerConfigurer template(Object template) {
        pollingEventListenerContainer.setTemplate(template);
        return this;
    }

    /**
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setPerformSnapshot(boolean)
     */
    public SimpleAsyncPollingContainerConfigurer performSnapshot(boolean performSnapshot) {
        pollingEventListenerContainer.setPerformSnapshot(performSnapshot);
        return this;
    }

    /**
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setTransactionManager(org.springframework.transaction.PlatformTransactionManager)
     */
    public SimpleAsyncPollingContainerConfigurer transactionManager(PlatformTransactionManager transactionManager) {
        pollingEventListenerContainer.setTransactionManager(transactionManager);
        return this;
    }

    /**
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setTransactionName(String)
     */
    public SimpleAsyncPollingContainerConfigurer transactionName(String transactionName) {
        pollingEventListenerContainer.setTransactionName(transactionName);
        return this;
    }

    /**
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setTransactionTimeout(int)
     */
    public SimpleAsyncPollingContainerConfigurer transactionTimeout(int transactionTimeout) {
        pollingEventListenerContainer.setTransactionTimeout(transactionTimeout);
        return this;
    }

    /**
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setTransactionIsolationLevel(int)
     */
    public SimpleAsyncPollingContainerConfigurer transactionIsolationLevel(int transactionIsolationLevel) {
        pollingEventListenerContainer.setTransactionIsolationLevel(transactionIsolationLevel);
        return this;
    }

    /**
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setExceptionHandler(org.openspaces.events.EventExceptionHandler)
     */
    public SimpleAsyncPollingContainerConfigurer exceptionHandler(EventExceptionHandler exceptionHandler) {
        pollingEventListenerContainer.setExceptionHandler(exceptionHandler);
        return this;
    }

    /**
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setEventListener(org.openspaces.events.SpaceDataEventListener)
     */
    public SimpleAsyncPollingContainerConfigurer eventListener(SpaceDataEventListener eventListener) {
        pollingEventListenerContainer.setEventListener(eventListener);
        return this;
    }

    /**
     * Sets an event listener that uses annotations
     *
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setEventListener(org.openspaces.events.SpaceDataEventListener)
     * @see org.openspaces.events.adapter.AnnotationEventListenerAdapter
     */
    public SimpleAsyncPollingContainerConfigurer eventListenerAnnotation(Object eventListener) {
        AnnotationEventListenerAdapter annotationEventListenerAdapter = new AnnotationEventListenerAdapter();
        annotationEventListenerAdapter.setDelegate(eventListener);
        annotationEventListenerAdapter.afterPropertiesSet();
        pollingEventListenerContainer.setEventListener(annotationEventListenerAdapter);
        return this;
    }

    /**
     * Sets an event listener that uses method name as an adapter
     *
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setEventListener(org.openspaces.events.SpaceDataEventListener)
     * @see org.openspaces.events.adapter.MethodEventListenerAdapter
     */
    public SimpleAsyncPollingContainerConfigurer eventListenerMethod(Object eventListener, String methodName) {
        MethodEventListenerAdapter methodEventListenerAdapter = new MethodEventListenerAdapter();
        methodEventListenerAdapter.setDelegate(eventListener);
        methodEventListenerAdapter.setMethodName(methodName);
        methodEventListenerAdapter.afterPropertiesSet();
        pollingEventListenerContainer.setEventListener(methodEventListenerAdapter);
        return this;
    }

    /**
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setActiveWhenPrimary(boolean)
     */
    public SimpleAsyncPollingContainerConfigurer activeWhenPrimary(boolean activeWhenPrimary) {
        pollingEventListenerContainer.setActiveWhenPrimary(activeWhenPrimary);
        return this;
    }

    /**
     * @see org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer#setAutoStart(boolean)
     */
    public SimpleAsyncPollingContainerConfigurer autoStart(boolean autoStart) {
        pollingEventListenerContainer.setAutoStart(autoStart);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setDynamicTemplate(Object)
     */
    public SimpleAsyncPollingContainerConfigurer dynamicTemplate(DynamicEventTemplateProvider templateProvider) {
        pollingEventListenerContainer.setDynamicTemplate(templateProvider);
        return this;
    }

    /**
     * @see org.openspaces.events.adapter.MethodDynamicEventTemplateProviderAdapter
     */
    public SimpleAsyncPollingContainerConfigurer dynamicTemplateMethod(Object templateProvider, String methodName) {
        MethodDynamicEventTemplateProviderAdapter adapter = new MethodDynamicEventTemplateProviderAdapter();
        adapter.setDelegate(templateProvider);
        adapter.setMethodName(methodName);
        adapter.afterPropertiesSet();
        return dynamicTemplate(adapter);
    }

    /**
     * @see org.openspaces.events.adapter.AnnotationDynamicEventTemplateProviderAdapter
     */
    public SimpleAsyncPollingContainerConfigurer dynamicTemplateAnnotation(Object templateProvider) {
        AnnotationDynamicEventTemplateProviderAdapter adapter = new AnnotationDynamicEventTemplateProviderAdapter();
        adapter.setDelegate(templateProvider);
        adapter.afterPropertiesSet();
        return dynamicTemplate(adapter);
    }

    /**
     * Creates a new {@link SimpleAsyncPollingEventListenerContainer} instance.
     */
    public SimpleAsyncPollingEventListenerContainer create() {
        if (!initialized) {
            pollingEventListenerContainer.setRegisterSpaceModeListener(true);
            pollingEventListenerContainer.afterPropertiesSet();
            initialized = true;
        }
        return pollingEventListenerContainer;
    }

    /**
     * Creates a new {@link SimpleAsyncPollingEventListenerContainer} instance.
     *
     * @see #create()
     */
    public SimpleAsyncPollingEventListenerContainer pollingContainer() {
        return create();
    }
}