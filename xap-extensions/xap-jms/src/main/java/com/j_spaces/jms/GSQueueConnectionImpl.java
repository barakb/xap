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
 * Created on 13/04/2004
 * 
 * @author Gershon Diner Title: The GigaSpaces Platform Copyright: Copyright (c)
 * GigaSpaces Team 2004 Company: GigaSpaces Technologies Ltd.
 * 
 * @version 4.0
 */
package com.j_spaces.jms;

import javax.jms.ConnectionConsumer;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Topic;

/**
 * Implements the <code>javax.jms.QueueConnection</code> interface.
 *
 * GigaSpaces implementation of the JMS QueueConnection Inerface. <p> <blockquote>
 *
 * <pre>
 *
 *  It also extends the GSConnectionImpl and overwrites most of its methods.
 *  - It holds the list of the sessions that attached to this conn.
 *  - It returns a LocalTransaction instance
 *  - it starts, stops and closes the Connection
 *
 * </p>
 * &lt;/blockquote&gt;
 * </pre>
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class GSQueueConnectionImpl extends GSConnectionImpl {

    /**
     * Constructor: Calling super com.j_spaces.jms.GSConnectionImpl.
     *
     * @param _connFacParent connection factory
     */
    public GSQueueConnectionImpl(GSConnectionFactoryImpl _connFacParent) throws JMSException {
        super(_connFacParent);
    }


    /**
     * When called on a <code>QueueConnection</code> it throws <code>IllegalStateException</code>.
     *
     * @see javax.jms.Connection#createDurableConnectionConsumer(Topic, String, String,
     * ServerSessionPool, int)
     */
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic,
                                                              String subscriptionName,
                                                              String messageSelector,
                                                              ServerSessionPool sessionPool,
                                                              int maxMessages)
            throws JMSException {
        throw new IllegalStateException("Forbidden call on a QueueConnection.");
    }

}//end of class
