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

/*
 * Created on 01/09/2004
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.j_spaces.jms;

import com.j_spaces.core.IJSpace;
import com.j_spaces.kernel.log.JProperties;
import com.j_spaces.worker.IPrivilegeWorker;

import java.util.Properties;

import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JMS_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JMS_ENABLED_PROP;

/**
 * @author yurak
 *
 *         TODO To change the template for this generated type comment go to Window - Preferences -
 *         Java - Code Style - Code Templates
 */
public class DurableServiceWorkerImpl
        implements IPrivilegeWorker {
    //constants for internal use only
    public final static String NUMBER_OF_MONITORED_TOPICS = "NUMBER_OF_MONITORED_TOPICS";
    public final static String NUMBER_OF_DURABLE_SUBSCRIBERS = "NUMBER_OF_DURABLE_SUBSCRIBERS";

    //internal members
    private JMSDurableSubService jmsDurableService = null;

    private boolean isJMSEnabled;

    /* (non-Javadoc)
     * @see com.j_spaces.worker.IWorker#init(com.j_spaces.core.IJSpace, java.lang.String, java.lang.String)
     */
    public void init(IJSpace proxy, String workerName, String arg) throws Exception {
        String containerName = proxy.getContainerName();
        isJMSEnabled = Boolean.valueOf(JProperties.getContainerProperty(containerName,
                LOOKUP_JMS_ENABLED_PROP, LOOKUP_JMS_ENABLED_DEFAULT)).booleanValue();
        //System.out.println("DurableServiceWorkerImpl: isJMSEnabled: " + isJMSEnabled);
        if (isJMSEnabled) {
            this.jmsDurableService = new JMSDurableSubService(proxy);
            //jmsDurableService.start();
        }
    }

    /* (non-Javadoc)
     * @see com.j_spaces.worker.IWorker#getInfo()
     */
    public Properties getInfo() {
        Properties props = new Properties();

        props.setProperty(NUMBER_OF_MONITORED_TOPICS, Integer.toString(0));
        props.setProperty(NUMBER_OF_DURABLE_SUBSCRIBERS, Integer.toString(0));

        return props;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.worker.IWorker#close()
     */
    public void close() {
        if (isJMSEnabled)
            jmsDurableService.shutdown();
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    public void run() {
        if (isJMSEnabled)
            jmsDurableService.run();
    }

}
