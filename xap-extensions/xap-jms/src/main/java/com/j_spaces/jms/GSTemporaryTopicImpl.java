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
 * Created on 01/06/2004
 *
 * @author		 	Gershon Diner
 * Title:        	 	The GigaSpaces Platform
 * Copyright:    	Copyright (c) GigaSpaces Team 2004
 * Company:      	GigaSpaces Technologies Ltd.
 * @version 	 	4.0
 */
package com.j_spaces.jms;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

/**
 * GigaSpaces implementation of the <code>javax.jms.TemporaryTopic</code> interface. This class
 * represents a queue which provides it's messages exclusively to one consumer at a time. In our
 * case, the Topic name represents the GigaSpaces entry name. We use the same space proxy we
 * obtained in the session and we write another External Entry, which will have a class name of
 * 'TempTopic'.
 *
 * The only message consumers that can consume from a temporary destination are those created by the
 * same connection that created the destination. Any message producer can send to the temporary
 * destination. If you close the connection that a temporary destination belongs to, the destination
 * is closed and its contents lost. You can use temporary destinations to implement a simple
 * request/reply mechanism. If you create a temporary destination and specify it as the value of the
 * JMSReplyTo message header field when you send a message, the consumer of the message can use the
 * value of the JMSReplyTo field as the destination to which it sends a reply and can also reference
 * the original request by setting the JMSCorrelationID header field of the reply message to the
 * value of the JMSMessageID header field of the request.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class GSTemporaryTopicImpl extends GSTopicImpl implements TemporaryTopic, Serializable {
    private static final long serialVersionUID = -212141426725056689L;

    /** The session the queue belongs to, <code>null</code> if not known. */
    //private GSSessionImpl session;

    /**
     * returns the source id of the object which have created this temp destination at first place.
     * It might be a specific MessageProducer or MessageConsumere etc. Note that by default we use
     * the session id as the source id, but when the client creates new instance it is his
     * responsability to set the sourceID with the Consumer/Producer ID and we should ne use the
     * session as the src id later.
     */
    String sourceID = "";

    /**
     * @param topicName
     * @param srcID
     */
    public GSTemporaryTopicImpl(String topicName, String srcID/*, GSSessionImpl session*/) {
        super(TEMP_TOPIC_PREFIX + topicName);
        //this.session = session;
        this.sourceID = srcID;
    }

    /**
     * We clear this temp external entry from space.
     *
     * @see TemporaryTopic#delete()
     */
    public void delete() throws JMSException {
//		try
//		{
//			session.conn.getSpace().clear( session.prepareJMSTemplateEntry(getTopicName()), null);
//		}
//		catch(EntryAlreadyInSpaceException entryInSpaceException)
//		{
//			   //doing nothing if the TempTopic already exist
//		}
//		catch(TransactionException te)
//		{
//			   JMSException e = 
//					new JMSException("TransactionException in GSTemporaryTopicImpl.delete() "+te.getMessage());
//			   e.setLinkedException( te);
//			   throw e;
//		}
//		catch(RemoteException re)
//		{
//				JMSException e = 
//						new JMSException("RemoteException in GSTemporaryTopicImpl.delete "+re.getMessage());
//				e.setLinkedException( re);
//				throw e;
//		} 
//		catch (UnusableEntryException ue)
//		{
//			JMSException e = 
//						new JMSException("UnusableEntryException in GSTemporaryTopicImpl.delete "+ue.getMessage());
//				e.setLinkedException( ue);
//				throw e;
//		}
    }

    /**
     * returns the source id of the object which have created this temp destination at first place.
     * It might be a specific MessageProducer or MessageConsumere etc.
     *
     * @return sourceID
     */
    public String getSourceID() {
        return sourceID;
    }

    /**
     * @param string
     */
    public void setSourceID(String string) {
        this.sourceID = string;
    }


    public String getDescription() {
        return "GSTemporaryTopicImpl  | Destination name:  " +
                toString() +
                "  |  sourceID: " + sourceID;
    }

}
