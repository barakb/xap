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
 * @author			Gershon Diner
 * Title:        		The GigaSpaces Platform
 * Copyright:    	Copyright (c) GigaSpaces Team 2004
 * Company:      GigaSpaces Technologies Ltd.
 * @version 	 	4.0
 */
package com.j_spaces.jms;

import javax.jms.JMSException;


/**
 * GigaSpaces implementation of the <code>javax.jms.QueueReceiver</code> interface.
 *
 * A client uses a <CODE>QueueReceiver</CODE> object to receive messages that have been delivered to
 * a queue.
 *
 * <P>Although it is possible to have multiple <CODE>QueueReceiver</CODE>s for the same queue, the
 * JMS API does not define how messages are distributed between the <CODE>QueueReceiver</CODE>s. The
 * GigaSpaces API does not support multiple QueueReceivers for the same queue.
 *
 * <P>If a <CODE>QueueReceiver</CODE> specifies a message selector, the messages that are not
 * selected remain on the queue. By definition, a message selector allows a
 * <CODE>QueueReceiver</CODE> to skip messages. This means that when the skipped messages are
 * eventually read, the total ordering of the reads does not retain the partial order defined by
 * each message producer. Only <CODE>QueueReceiver</CODE>s without a message selector will read
 * messages in message producer order.
 *
 * <P>Creating a <CODE>MessageConsumer</CODE> provides the same features as creating a
 * <CODE>QueueReceiver</CODE>. A <CODE>MessageConsumer</CODE> object is recommended for creating new
 * code. The  <CODE>QueueReceiver</CODE> is provided to support existing code.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 * @see javax.jms.Session#createConsumer(Destination, String)
 * @see javax.jms.Session#createConsumer(Destination)
 * @see javax.jms.QueueSession#createReceiver(Queue, String)
 * @see javax.jms.QueueSession#createReceiver(Queue)
 * @see javax.jms.MessageConsumer
 */
public class GSQueueReceiverImpl
        extends GSMessageConsumerImpl {
    /**
     * @param sess
     * @param queue
     * @param consumerKey
     * @param selector
     * @throws JMSException
     */
    public GSQueueReceiverImpl(GSSessionImpl sess,
                               GSQueueImpl queue,
                               String consumerKey,
                               String selector) throws JMSException {
        super(sess, queue, consumerKey, null, selector, false);
    }
}//end of class