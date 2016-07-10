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
 * Created on 11/03/2004
 * 
 * @author		Gershon Diner
 * Title:      	The GigaSpaces Platform
 * Copyright:  	Copyright (c) GigaSpaces Team 2004
 * Company:   	GigaSpaces Technologies Ltd.
 * @version 	4.0
 */
package com.j_spaces.jms;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.TemporaryQueue;

/**
 * GigaSpaces implementation of the <code>javax.jms.TopicSession</code> interface.
 *
 * A <CODE>TopicSession</CODE> object provides methods for creating <CODE>
 * TopicPublisher</CODE>,<CODE>TopicSubscriber</CODE>( and <CODE> TemporaryTopic</CODE>) objects. It
 * also provides a method for deleting its client's durable subscribers.
 *
 * <P> A <CODE>TopicSession</CODE> is used for creating Pub/Sub specific objects. In general, use
 * the <CODE>Session</CODE> object, and use <CODE> TopicSession</CODE> only to support existing
 * code. Using the <CODE>Session </CODE> object simplifies the programming model, and allows
 * transactions to be used across the two messaging domains.
 *
 * <P> A <CODE>TopicSession</CODE> cannot be used to create objects specific to the point-to-point
 * domain. The following methods inherit from <CODE>Session </CODE>, but must throw an
 * <CODE>IllegalStateException</CODE> if used from <CODE>TopicSession</CODE>: <UL>
 * <LI><CODE>createBrowser</CODE> <LI><CODE>createQueue</CODE> <LI><CODE>createTemporaryQueue</CODE>
 * </UL>
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class GSTopicSessionImpl extends GSSessionImpl
        //implements TopicSession
{
    /**
     * Constructs a topic session.
     *
     * @param connection      The connection the session belongs to.
     * @param transacted      <code>true</code> for a transacted session.
     * @param acknowledgeMode 1 (auto), 2 (client) or 3 (dups ok).
     * @throws JMSException In case of an invalid acknowledge mode.
     */
    public GSTopicSessionImpl(GSConnectionImpl connection, boolean transacted,
                              int acknowledgeMode) throws JMSException {
        super(connection, transacted, acknowledgeMode);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.jms.Session#createQueue(java.lang.String)
     */
    public Queue createQueue(String arg0) throws JMSException {
        throw new IllegalStateException("Forbidden call on a TopicSession.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.jms.Session#createBrowser(javax.jms.Queue)
     */
    public QueueBrowser createBrowser(Queue arg0) throws JMSException {
        throw new IllegalStateException("Forbidden call on a TopicSession.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.jms.Session#createBrowser(javax.jms.Queue, java.lang.String)
     */
    public QueueBrowser createBrowser(Queue arg0, String arg1)
            throws JMSException {
        throw new IllegalStateException("Forbidden call on a TopicSession.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.jms.Session#createTemporaryQueue()
     */
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new IllegalStateException("Forbidden call on a TopicSession.");
    }

}//end of class
