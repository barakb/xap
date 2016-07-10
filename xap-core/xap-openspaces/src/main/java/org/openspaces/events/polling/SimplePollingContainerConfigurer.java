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


package org.openspaces.events.polling;

import org.openspaces.core.GigaSpace;
import org.openspaces.events.DynamicEventTemplateProvider;
import org.openspaces.events.EventExceptionHandler;
import org.openspaces.events.SpaceDataEventListener;
import org.openspaces.events.adapter.AnnotationDynamicEventTemplateProviderAdapter;
import org.openspaces.events.adapter.AnnotationEventListenerAdapter;
import org.openspaces.events.adapter.MethodDynamicEventTemplateProviderAdapter;
import org.openspaces.events.adapter.MethodEventListenerAdapter;
import org.openspaces.events.polling.receive.ReceiveOperationHandler;
import org.openspaces.events.polling.trigger.TriggerOperationHandler;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * A simplified programmatic configuration that for {@link org.openspaces.events.polling.SimplePollingEventListenerContainer}.
 *
 * <p>Sample usage:
 * <pre>
 * UrlSpaceConfigurer urlSpaceConfigurerPrimary = new UrlSpaceConfigurer("/./space");
 * GigaSpace gigaSpace = new GigaSpaceConfigurer(urlSpaceConfigurerPrimary.space()).gigaSpace();
 * SimplePollingEventListenerContainer pollingEventListenerContainer = new
 * SimplePollingContainerConfigurer(gigaSpace)
 *              .template(new TestMessage())
 *              .eventListenerAnnotation(new Object() {
 *                  <code>@SpaceDataEvent</code> public void gotMeselfAnEvent() {
 *                      // do something
 *                  }
 *              }).notifyContainer();
 *
 * ...
 *
 * pollingEventListenerContainer.destroy();
 * urlSpaceConfigurerPrimary.destroy();
 * </pre>
 *
 * @author kimchy
 */
public class SimplePollingContainerConfigurer {

    private final SimplePollingEventListenerContainer pollingEventListenerContainer;

    private boolean initialized = false;

    public SimplePollingContainerConfigurer(GigaSpace gigaSpace) {
        pollingEventListenerContainer = new SimplePollingEventListenerContainer();
        pollingEventListenerContainer.setGigaSpace(gigaSpace);
    }

    public SimplePollingContainerConfigurer name(String name) {
        pollingEventListenerContainer.setBeanName(name);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setTaskExecutor(org.springframework.core.task.TaskExecutor)
     */
    public SimplePollingContainerConfigurer taskExecutor(TaskExecutor taskExecutor) {
        pollingEventListenerContainer.setTaskExecutor(taskExecutor);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setRecoveryInterval(long)
     */
    public SimplePollingContainerConfigurer recoveryInterval(long recoveryInterval) {
        pollingEventListenerContainer.setRecoveryInterval(recoveryInterval);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setConcurrentConsumers(int)
     */
    public SimplePollingContainerConfigurer concurrentConsumers(int concurrentConsumers) {
        pollingEventListenerContainer.setConcurrentConsumers(concurrentConsumers);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setMaxConcurrentConsumers(int)
     */
    public SimplePollingContainerConfigurer maxConcurrentConsumers(int maxConcurrentConsumers) {
        pollingEventListenerContainer.setMaxConcurrentConsumers(maxConcurrentConsumers);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setMaxEventsPerTask(int)
     */
    public SimplePollingContainerConfigurer maxEventsPerTask(int maxEventsPerTask) {
        pollingEventListenerContainer.setMaxEventsPerTask(maxEventsPerTask);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setIdleTaskExecutionLimit(int)
     */
    public SimplePollingContainerConfigurer idleTaskExecutionLimit(int idleTaskExecutionLimit) {
        pollingEventListenerContainer.setIdleTaskExecutionLimit(idleTaskExecutionLimit);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setPassArrayAsIs(boolean)
     */
    public SimplePollingContainerConfigurer passArrayAsIs(boolean passArrayAsIs) {
        pollingEventListenerContainer.setPassArrayAsIs(passArrayAsIs);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setReceiveTimeout(long)
     */
    public SimplePollingContainerConfigurer receiveTimeout(long receiveTimeout) {
        pollingEventListenerContainer.setReceiveTimeout(receiveTimeout);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setReceiveOperationHandler(org.openspaces.events.polling.receive.ReceiveOperationHandler)
     */
    public SimplePollingContainerConfigurer receiveOperationHandler(ReceiveOperationHandler receiveOperationHandler) {
        pollingEventListenerContainer.setReceiveOperationHandler(receiveOperationHandler);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setTriggerOperationHandler(org.openspaces.events.polling.trigger.TriggerOperationHandler)
     */
    public SimplePollingContainerConfigurer triggerOperationHandler(TriggerOperationHandler triggerOperationHandler) {
        pollingEventListenerContainer.setTriggerOperationHandler(triggerOperationHandler);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setTemplate(Object)
     */
    public SimplePollingContainerConfigurer template(Object template) {
        pollingEventListenerContainer.setTemplate(template);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setPerformSnapshot(boolean)
     */
    public SimplePollingContainerConfigurer performSnapshot(boolean performSnapshot) {
        pollingEventListenerContainer.setPerformSnapshot(performSnapshot);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setTransactionManager(org.springframework.transaction.PlatformTransactionManager)
     */
    public SimplePollingContainerConfigurer transactionManager(PlatformTransactionManager transactionManager) {
        pollingEventListenerContainer.setTransactionManager(transactionManager);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setTransactionName(String)
     */
    public SimplePollingContainerConfigurer transactionName(String transactionName) {
        pollingEventListenerContainer.setTransactionName(transactionName);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setTransactionTimeout(int)
     */
    public SimplePollingContainerConfigurer transactionTimeout(int transactionTimeout) {
        pollingEventListenerContainer.setTransactionTimeout(transactionTimeout);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setTransactionIsolationLevel(int)
     */
    public SimplePollingContainerConfigurer transactionIsolationLevel(int transactionIsolationLevel) {
        pollingEventListenerContainer.setTransactionIsolationLevel(transactionIsolationLevel);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setExceptionHandler(org.openspaces.events.EventExceptionHandler)
     */
    public SimplePollingContainerConfigurer exceptionHandler(EventExceptionHandler exceptionHandler) {
        pollingEventListenerContainer.setExceptionHandler(exceptionHandler);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setEventListener(org.openspaces.events.SpaceDataEventListener)
     */
    public SimplePollingContainerConfigurer eventListener(SpaceDataEventListener eventListener) {
        pollingEventListenerContainer.setEventListener(eventListener);
        return this;
    }

    /**
     * Sets an event listener that uses annotations
     *
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setEventListener(org.openspaces.events.SpaceDataEventListener)
     * @see org.openspaces.events.adapter.AnnotationEventListenerAdapter
     */
    public SimplePollingContainerConfigurer eventListenerAnnotation(Object eventListener) {
        AnnotationEventListenerAdapter annotationEventListenerAdapter = new AnnotationEventListenerAdapter();
        annotationEventListenerAdapter.setDelegate(eventListener);
        annotationEventListenerAdapter.afterPropertiesSet();
        pollingEventListenerContainer.setEventListener(annotationEventListenerAdapter);
        return this;
    }

    /**
     * Sets an event listener that uses method name as an adapter
     *
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setEventListener(org.openspaces.events.SpaceDataEventListener)
     * @see org.openspaces.events.adapter.MethodEventListenerAdapter
     */
    public SimplePollingContainerConfigurer eventListenerMethod(Object eventListener, String methodName) {
        MethodEventListenerAdapter methodEventListenerAdapter = new MethodEventListenerAdapter();
        methodEventListenerAdapter.setDelegate(eventListener);
        methodEventListenerAdapter.setMethodName(methodName);
        methodEventListenerAdapter.afterPropertiesSet();
        pollingEventListenerContainer.setEventListener(methodEventListenerAdapter);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setActiveWhenPrimary(boolean)
     */
    public SimplePollingContainerConfigurer activeWhenPrimary(boolean activeWhenPrimary) {
        pollingEventListenerContainer.setActiveWhenPrimary(activeWhenPrimary);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setAutoStart(boolean)
     */
    public SimplePollingContainerConfigurer autoStart(boolean autoStart) {
        pollingEventListenerContainer.setAutoStart(autoStart);
        return this;
    }

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setDynamicTemplate(Object)
     */
    public SimplePollingContainerConfigurer dynamicTemplate(DynamicEventTemplateProvider templateProvider) {
        pollingEventListenerContainer.setDynamicTemplate(templateProvider);
        return this;
    }

    /**
     * @see org.openspaces.events.adapter.MethodDynamicEventTemplateProviderAdapter
     */
    public SimplePollingContainerConfigurer dynamicTemplateMethod(Object templateProvider, String methodName) {
        MethodDynamicEventTemplateProviderAdapter adapter = new MethodDynamicEventTemplateProviderAdapter();
        adapter.setDelegate(templateProvider);
        adapter.setMethodName(methodName);
        adapter.afterPropertiesSet();
        return dynamicTemplate(adapter);
    }

    /**
     * @see org.openspaces.events.adapter.AnnotationDynamicEventTemplateProviderAdapter
     */
    public SimplePollingContainerConfigurer dynamicTemplateAnnotation(Object templateProvider) {
        AnnotationDynamicEventTemplateProviderAdapter adapter = new AnnotationDynamicEventTemplateProviderAdapter();
        adapter.setDelegate(templateProvider);
        adapter.afterPropertiesSet();
        return dynamicTemplate(adapter);
    }

    /**
     * Creates a new {@link SimplePollingEventListenerContainer} instance.
     */
    public SimplePollingEventListenerContainer create() {
        if (!initialized) {
            pollingEventListenerContainer.setRegisterSpaceModeListener(true);
            pollingEventListenerContainer.afterPropertiesSet();
            initialized = true;
        }
        return pollingEventListenerContainer;
    }

    /**
     * Creates a new {@link SimplePollingEventListenerContainer} instance.
     *
     * @see #create()
     */
    public SimplePollingEventListenerContainer pollingContainer() {
        return create();
    }
}
