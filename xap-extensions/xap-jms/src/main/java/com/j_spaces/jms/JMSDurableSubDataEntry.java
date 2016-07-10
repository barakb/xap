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
 * Created on 01/08/2004
 *
 * @author		 Gershon Diner
 * Title:        The GigaSpaces Platform
 * Copyright:    Copyright (c) GigaSpaces Team 2004
 * Company:      GigaSpaces Technologies Ltd.
 * @version 	 4.0
 */
package com.j_spaces.jms;

import com.j_spaces.core.client.IReplicatable;
import com.j_spaces.core.client.MetaDataEntry;

import java.util.Vector;

/**
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 **/
public class JMSDurableSubDataEntry extends MetaDataEntry implements IReplicatable {
    private static final long serialVersionUID = -8034588332136909021L;

    public String m_durableSubscriptionName;

    /* The topic name that the durable subscriber should subscribe to
     * and which is subscribed with the m_durableSubscriptionName unique name. */
    public String m_topicName;

    /* if true then this durable topic is currently subscribed
     * with the m_durableSubscriptionName unique name.
     * if false, then this durableTopic was unsubscribed
     * and no longer act as a durable subscription to this topic.*/
    public Boolean m_isSubscribed;

    /* if true then this durable topic is currently subscribed
     * with the m_durableSubscriptionName unique name, and
     * the topic subscriber is currently online.
     * if false, then this durable Topic subscriber is currently offline
     * and it indicates to the JMSDurableSubService to collect all
     * the entry's while the durable subscriber is offline.*/
    public Boolean m_isSubscriberOnline;

    /* the consumer client id which identifies each
     * durable subsriber with a specific space name and
     * topic name (e.g. SpaceName_TopicName).
     * We later create single specific durable consumer subscription
     * to a topic, according to this consumer client id. */
    public String m_subscriberClientID;

    /* Vector contains all the ExternalEntry UID's which where consumed from the
     *  m_durableTopic by the DurableSubscriber,
     * while it was offline (e.g. closed/shutdown). */
    public Vector m_offlineEntryUIDsVec;

    public JMSDurableSubDataEntry() {
        setFifo(true);
    }

//	public static String[] __getSpaceIndexedFields()
//	{
//		String[] indexedFields = { "m_durableSubscriptionName, m_subscriberClientID" };
//
//		return indexedFields;
//	}

    public String toString() {
        return "	JMSDurableSubDataEntry " +
                " | m_durableSubscriptionName=  " + m_durableSubscriptionName +
                " | m_subscriberClientID=  " + m_subscriberClientID +
                " | m_isSubscribed=  " + m_isSubscribed +
                " | m_isSubscriberOnline=  " + m_isSubscriberOnline +
                " | m_offlineEntryUIDsVec=  " + m_offlineEntryUIDsVec;
    }

}
