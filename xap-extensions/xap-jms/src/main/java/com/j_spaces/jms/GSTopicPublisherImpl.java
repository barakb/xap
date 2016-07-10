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
 * Title:        	The GigaSpaces Platform
 * Copyright:   Copyright (c) GigaSpaces Team 2004
 * Company:    GigaSpaces Technologies Ltd.
 * @version 	 4.0
 */
package com.j_spaces.jms;

import com.j_spaces.jms.utils.IMessageConverter;

import javax.jms.JMSException;

/**
 * GigaSpaces implementation of the <code>javax.jms.TopicPublisher</code> interface.
 *
 * A client uses a <CODE>GSTopicPublisherImpl</CODE> object to publish messages on a topic. A
 * <CODE>GSTopicPublisherImpl</CODE> object is the publish-subscribe form of a message producer.
 *
 * <P>Normally, the <CODE>Topic</CODE> is specified when a <CODE>GSTopicPublisherImpl</CODE> is
 * created.  In this case, an attempt to use the <CODE>publish</CODE> methods for an unidentified
 * <CODE>GSTopicPublisherImpl</CODE> will throw a <CODE>java.lang.UnsupportedOperationException</CODE>.
 *
 * <P>If the <CODE>GSTopicPublisherImpl</CODE> is created with an unidentified <CODE>Topic</CODE>,
 * an attempt to use the <CODE>publish</CODE> methods that assume that the <CODE>Topic</CODE> has
 * been identified will throw a <CODE>java.lang.UnsupportedOperationException</CODE>.
 *
 * <P>During the execution of its <CODE>publish</CODE> method, a message must not be changed by
 * other threads within the client. If the message is modified, the result of the
 * <CODE>publish</CODE> is undefined.
 *
 * <P>After publishing a message, a client may retain and modify it without affecting the message
 * that has been published. The same message object may be published multiple times.
 *
 * <P>The following message headers are set as part of publishing a message:
 * <code>JMSDestination</code>, <code>JMSDeliveryMode</code>, <code>JMSExpiration</code>,
 * <code>JMSPriority</code>, <code>JMSMessageID</code> and <code>JMSTimeStamp</code>. When the
 * message is published, the values of these headers are ignored. After completion of the
 * <CODE>publish</CODE>, the headers hold the values specified by the method publishing the message.
 * It is possible for the <CODE>publish</CODE> method not to set <code>JMSMessageID</code> and
 * <code>JMSTimeStamp</code> if the setting of these headers is explicitly disabled by the
 * <code>GSMessageProducerImpl.setDisableMessageID</code> or <code>GSMessageProducerImpl.setDisableMessageTimestamp</code>
 * method.
 *
 * <P>Creating a <CODE>GSMessageProducerImpl</CODE> provides the same features as creating a
 * <CODE>GSTopicPublisherImpl</CODE>. A <CODE>GSMessageProducerImpl</CODE> object is recommended
 * when creating new code. The  <CODE>GSTopicPublisherImpl</CODE> is provided to support existing
 * code.
 *
 *
 * <P>Because <CODE>GSTopicPublisherImpl</CODE> inherits from <CODE>GSMessageProducerImpl</CODE>, it
 * inherits the <CODE>send</CODE> methods that are a part of the <CODE>GSMessageProducerImpl</CODE>
 * interface. Using the <CODE>send</CODE> methods will have the same effect as using the
 * <CODE>publish</CODE> methods: they are functionally the same.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class GSTopicPublisherImpl
        extends GSMessageProducerImpl {

    /**
     * Constructs a publisher.
     *
     * @param sess  The session the publisher belongs to.
     * @param topic The topic the publisher publishs messages on.
     * @throws IllegalStateException If the connection is broken.
     * @throws JMSException          If the creation fails for any other reason.
     */
    public GSTopicPublisherImpl(GSSessionImpl sess,
                                GSTopicImpl topic,
                                IMessageConverter messageConverter)
            throws JMSException {
        super(sess, topic, messageConverter);
    }
}//end of class