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


package org.openspaces.esb.mule.eventcontainer;

import org.mule.api.MuleContext;
import org.mule.api.MuleException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.transport.AbstractConnector;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * An OpenSpaces mule connector holding Spring application context which is later used by the
 * receiver and the dispatcher to lookup registered beans within the application context. For
 * example, the dispatcher looks up a <code>GigaSpace</code> instance in order to send the code
 * using it.
 *
 * <p>Note, the connector must be defined within mule configuration in order for it to be injected
 * with the application context.
 *
 * @author yitzhaki
 */
public class OpenSpacesConnector extends AbstractConnector implements ApplicationContextAware {


    public static final String OS_EVENT_CONTAINER = "os-eventcontainer";

    private ApplicationContext applicationContext;

    public OpenSpacesConnector(MuleContext context) {
        super(context);
    }

    /**
     * @return the openspaces protocol name.
     */
    public String getProtocol() {
        return OS_EVENT_CONTAINER;
    }


    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    protected void doInitialise() throws InitialisationException {
    }

    protected void doDispose() {
    }

    protected void doStart() throws MuleException {
    }

    protected void doStop() throws MuleException {
    }

    protected void doConnect() throws Exception {
    }

    protected void doDisconnect() throws Exception {
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public boolean isResponseEnabled() {
        return true;
    }

}
