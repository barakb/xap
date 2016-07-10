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
 * @author		Gershon Diner
 * Title:        		The GigaSpaces Platform
 * Copyright:    	Copyright (c) GigaSpaces Team 2004
 * Company:      GigaSpaces Technologies Ltd.
 * @version 	 	4.0
 */
package com.j_spaces.jms;

import java.io.Serializable;
import java.rmi.Remote;

import javax.jms.JMSException;
import javax.jms.Queue;

/**
 * GigaSpaces implementation of the <code>javax.jms.Queue</code> interface. This class represents a
 * queue which provides it's messages exclusively to one consumer at a time. In our case, the Queue
 * name represents the GigaSpaces entry name.
 *
 * A <CODE>Queue</CODE> object encapsulates a provider-specific queue name. It is the way a client
 * specifies the identity of a queue to JMS API methods.
 *
 * For those methods that use a <CODE>Destination</CODE> as a parameter, a <CODE>Queue</CODE> object
 * used as an argument. For example, a queue can be used  to create a <CODE>MessageConsumer</CODE>
 * and a <CODE>MessageProducer</CODE>  by calling: <UL> <LI> <CODE>Session.CreateConsumer(Destination
 * destination)</CODE> <LI> <CODE>Session.CreateProducer(Destination destination)</CODE>
 *
 * </UL>
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class GSQueueImpl
        implements Queue, Serializable, Remote {

    /** */
    private static final long serialVersionUID = 1L;
    String m_queueName;
    /**
     * This is the prefix used by the temporary queues
     */
    final static String TEMP_QUEUE_PREFIX = "TempQueue:";

    /**
     * @param queueName
     */
    public GSQueueImpl(String queueName) {
        this.m_queueName = queueName;
    }

    /**
     * @return String m_queueName
     * @see Queue#getQueueName()
     */
    public String getQueueName() throws JMSException {
        return m_queueName;
    }

    /**
     * This method determines whether a particular Destination instance refers to a temporary
     * destination.
     *
     * @return boolean             true if it is
     */
    protected boolean isTemporaryDestination() {
        boolean result = false;
        if (m_queueName.startsWith(TEMP_QUEUE_PREFIX)) {
            result = true;
        }
        return result;
    }

    public String toString() {
        return m_queueName;
    }

    /**
     * @see Object#equals(Object) Using the Destination name and return true if its equals to the
     * passed Destination name. That enables indexing and partitioning capabilities.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof com.j_spaces.jms.GSQueueImpl))
            return false;
        com.j_spaces.jms.GSQueueImpl otherObj = (com.j_spaces.jms.GSQueueImpl) obj;

        return m_queueName.equals(otherObj.toString());
    }

    /**
     * @see Object#hashCode() Using the Destination name and return its hashcode. That enables
     * indexing and partitioning capabilities.
     */
    @Override
    public int hashCode() {
       /*System.err.println("  ---- m_queueName: " + m_queueName
                          + " hashCode: " + m_queueName.hashCode());*/
        return m_queueName.hashCode();
    }

}//end of class
