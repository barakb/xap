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
 * Title:        		The GigaSpaces Platform
 * Copyright:    	Copyright (c) GigaSpaces Team 2004
 * Company:     GigaSpaces Technologies Ltd.
 * @version 	 	4.0
 */
package com.j_spaces.jms;

import java.io.Serializable;
import java.rmi.Remote;

import javax.jms.JMSException;
import javax.jms.Topic;

/**
 * GigaSpaces implementation of the <code>javax.jms.Topic</code> interface.
 *
 * A <CODE>Topic</CODE> object encapsulates a provider-specific topic name. In our case, the Topic
 * name represents the GigaSpaces entry name.
 *
 * It is the way a client specifies the identity of a topic to JMS API methods. For those methods
 * that use a <CODE>Destination</CODE> as a parameter, a <CODE>Topic</CODE> object may used as an
 * argument . For example, a Topic can be used to create a <CODE>MessageConsumer</CODE> and a
 * <CODE>MessageProducer</CODE> by calling: <UL> <LI> <CODE>Session.CreateConsumer(Destination
 * destination)</CODE> <LI> <CODE>Session.CreateProducer(Destination destination)</CODE>
 *
 * </UL>
 *
 * <P>Many publish/subscribe (pub/sub) providers group topics into hierarchies and provide various
 * options for subscribing to parts of the hierarchy. The JMS API places no restriction on what a
 * <CODE>Topic</CODE> object represents. It may be a leaf in a topic hierarchy, or it may be a
 * larger part of the hierarchy.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class GSTopicImpl
        implements Topic, Serializable, Remote {

    /** */
    private static final long serialVersionUID = 1L;
    String m_TopicName;
    /**
     * This is the prefix used by the temporary topics
     */
    final static String TEMP_TOPIC_PREFIX = "TempTopic:";

    /**
     * @param topicName
     */
    public GSTopicImpl(String topicName) {
        this.m_TopicName = topicName;
    }

    /**
     * @return String m_TopicName
     * @see Topic#getTopicName()
     */
    public String getTopicName() throws JMSException {
        return m_TopicName;
    }

    /**
     * This method determines whether a particular Destination instance refers to a temporary
     * destination.
     *
     * @return boolean             true if it is
     */
    protected boolean isTemporaryDestination() {
        boolean result = false;
        if (m_TopicName.startsWith(TEMP_TOPIC_PREFIX)) {
            result = true;
        }
        return result;
    }

    public String toString() {
        return m_TopicName;
    }

    /**
     * @see Object#equals(Object) Using the Destination name and return true if its equals to the
     * passed Destination name. That enables indexing and partitioning capabilities.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof com.j_spaces.jms.GSTopicImpl))
            return false;
        com.j_spaces.jms.GSTopicImpl otherObj = (com.j_spaces.jms.GSTopicImpl) obj;

        return m_TopicName.equals(otherObj.toString());
    }

    /**
     * @see Object#hashCode() Using the Destination name and return its hashcode. That enables
     * indexing and partitioning capabilities.
     */
    @Override
    public int hashCode() {
       /*System.err.println("  ---- m_TopicName: " + m_TopicName
                          + " hashCode: " + m_TopicName.hashCode());*/
        return m_TopicName.hashCode();
    }


}//end of class
