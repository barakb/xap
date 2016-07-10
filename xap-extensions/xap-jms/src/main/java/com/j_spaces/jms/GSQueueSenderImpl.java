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

import com.j_spaces.jms.utils.IMessageConverter;

import javax.jms.JMSException;

/**
 * GigaSpaces implementation of the <code>javax.jms.Session</code> interface.
 *
 * A client uses a <CODE>GSQueueSenderImpl</CODE> object to send messages to a queue.
 *
 * <P>Normally, the <CODE>Queue</CODE> is specified when a <CODE>GSQueueSenderImpl</CODE> is
 * created.  In this case, an attempt to use the <CODE>send</CODE> methods for an unidentified
 * <CODE>GSQueueSenderImpl</CODE> will throw a <CODE>java.lang.UnsupportedOperationException</CODE>.
 *
 * <P>If the <CODE>GSQueueSenderImpl</CODE> is created with an unidentified <CODE>Queue</CODE>, an
 * attempt to use the <CODE>send</CODE> methods that assume that the <CODE>Queue</CODE> has been
 * identified will throw a <CODE>java.lang.UnsupportedOperationException</CODE>.
 *
 * <P>During the execution of its <CODE>send</CODE> method, a message must not be changed by other
 * threads within the client. If the message is modified, the result of the <CODE>send</CODE> is
 * undefined.
 *
 * <P>After sending a message, a client may retain and modify it without affecting the message that
 * has been sent. The same message object may be sent multiple times.
 *
 * <P>The following message headers are set as part of sending a message:
 * <code>JMSDestination</code>, <code>JMSDeliveryMode</code>, <code>JMSExpiration</code>,
 * <code>JMSPriority</code>, <code>JMSMessageID</code> and <code>JMSTimeStamp</code>. When the
 * message is sent, the values of these headers are ignored. After the completion of the
 * <CODE>send</CODE>, the headers hold the values specified by the method sending the message. It is
 * possible for the <code>send</code> method not to set <code>JMSMessageID</code> and
 * <code>JMSTimeStamp</code> if the setting of these headers is explicitly disabled by the
 * <code>MessageProducer.setDisableMessageID</code> or <code>MessageProducer.setDisableMessageTimestamp</code>
 * method.
 *
 * <P>Creating a <CODE>MessageProducer</CODE> provides the same features as creating a
 * <CODE>GSQueueSenderImpl</CODE>. A <CODE>MessageProducer</CODE> object is recommended when
 * creating new code. The  <CODE>GSQueueSenderImpl</CODE> is provided to support existing code.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 * @see javax.jms.MessageProducer
 * @see javax.jms.Session#createProducer(Destination)
 * @see javax.jms.QueueSession#createSender(Queue)
 */
public class GSQueueSenderImpl
        extends GSMessageProducerImpl {


    /**
     * Constructs a queue sender.
     *
     * @param sess  The session the publisher belongs to.
     * @param queue The queue the publisher publishs messages on.
     * @throws IllegalStateException If the connection is broken.
     * @throws JMSException          If the creation fails for any other reason.
     */
    public GSQueueSenderImpl(GSSessionImpl sess,
                             GSQueueImpl queue,
                             IMessageConverter messageConverter)
            throws JMSException {
        super(sess, queue, messageConverter);
    }
}//end of class