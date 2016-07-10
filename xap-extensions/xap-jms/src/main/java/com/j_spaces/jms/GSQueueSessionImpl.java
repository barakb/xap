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
 * Title:       The GigaSpaces Platform
 * Copyright:   Copyright (c) GigaSpaces Team 2004
 * Company:     GigaSpaces Technologies Ltd.
 * @version 	4.0
 */
package com.j_spaces.jms;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

/**
 * GigaSpaces implementation of the <code>javax.jms.QueueSession</code> interface.
 *
 * A <CODE>QueueSession</CODE> object provides methods for creating <CODE>QueueReceiver</CODE>,
 * <CODE>QueueSender</CODE>, TBD -- <CODE>QueueBrowser</CODE>, and <CODE>TemporaryQueue</CODE>
 * objects.
 *
 * <P>If there are messages that have been received but not acknowledged when a
 * <CODE>QueueSession</CODE> terminates, these messages will be retained and redelivered when a
 * consumer next accesses the queue.
 *
 * <P>A <CODE>QueueSession</CODE> is used for creating Point-to-Point specific objects. In general,
 * use the <CODE>Session</CODE> object. The <CODE>QueueSession</CODE> is used to support existing
 * code. Using the <CODE>Session</CODE> object simplifies the programming model, and allows
 * transactions to be used across the two messaging domains.
 *
 * <P>A <CODE>QueueSession</CODE> cannot be used to create objects specific to the publish/subscribe
 * domain. The following methods inherit from <CODE>Session</CODE>, but must throw an
 * <CODE>IllegalStateException</CODE> if they are used from <CODE>QueueSession</CODE>: <UL>
 * <LI><CODE>createDurableSubscriber</CODE> <LI><CODE>createTemporaryTopic</CODE>
 * <LI><CODE>createTopic</CODE> <LI><CODE>unsubscribe</CODE> </UL>
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 * @see javax.jms.Session
 * @see javax.jms.QueueConnection#createQueueSession(boolean, int)
 * @see javax.jms.XAQueueSession#getQueueSession()
 */
public class GSQueueSessionImpl extends GSSessionImpl// implements QueueSession
{
    /**
     * Constructs a queue session.
     *
     * @param connection      The connection the session belongs to.
     * @param transacted      <code>true</code> for a transacted session.
     * @param acknowledgeMode 1 (auto), 2 (client) or 3 (dups ok).
     * @throws JMSException In case of an invalid acknowledge mode.
     */
    public GSQueueSessionImpl(GSConnectionImpl connection,
                              boolean transacted,
                              int acknowledgeMode)
            throws JMSException {
        super(connection, transacted, acknowledgeMode);
    }


    /*
     * @see javax.jms.Session#createTemporaryTopic()
     */
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new IllegalStateException("Forbidden call on a QueueSession.");
    }


    /**
     * API method.
     *
     * @throws IllegalStateException Systematically.
     */
    public TopicSubscriber createDurableSubscriber(Topic topic,
                                                   String name,
                                                   String selector,
                                                   boolean noLocal)
            throws JMSException {
        throw new IllegalStateException("Forbidden call on a QueueSession.");
    }


    /**
     * API method.
     *
     * @throws IllegalStateException Systematically.
     */
    public TopicSubscriber createDurableSubscriber(Topic topic, String name)
            throws JMSException {
        throw new IllegalStateException("Forbidden call on a QueueSession.");
    }

    /**
     * API method.
     *
     * @throws IllegalStateException Systematically.
     */
    public Topic createTopic(String topicName) throws JMSException {
        throw new IllegalStateException("Forbidden call on a QueueSession.");
    }

    /**
     * API method.
     *
     * @throws IllegalStateException Systematically.
     */
    public void unsubscribe(String name) throws JMSException {
        throw new IllegalStateException("Forbidden call on a QueueSession.");
    }

}//end of class
