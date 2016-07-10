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
 * Created on 20/04/2004
 *
 * @author		Gershon Diner
 * Title:        	The GigaSpaces Platform
 * Copyright:   Copyright (c) GigaSpaces Team 2004
 * Company:    GigaSpaces Technologies Ltd.
 * @version	4.0
 */
package com.j_spaces.jms;

import com.j_spaces.core.client.IReplicatable;
import com.j_spaces.core.client.MetaDataEntry;

/**
 * A MetaDataEntry extension, that is used for jms message Acknowledging. This entry gets written to
 * space everytime an Ack is required (CLIENT_ACKNOLEDGE) and it is done from the
 * GSSessionImpl.acknoledge() which writes a series of JMSAckDataEntry entries to the space, and
 * there are ND listeners which are listening to the ACK callback. After the successful ACK, the
 * JMSAckDataEntry entry is clear()'ed from space.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class JMSAckDataEntry extends MetaDataEntry implements IReplicatable {
    private static final long serialVersionUID = 6405433952373133461L;

    transient public int m_ackMode;
    /**
     * the producer key that sent the message and is waiting for the Ack
     */
    transient public String m_producerKey;
    /**
     * the consumer key that the message was sent to and that performed the Ack
     */
    transient public String m_consumerKey;
    /** the returned ExternalEntry message uid that was sent and acked. *//*
   transient public String m_ackedExternalEntryUID;*/
    /**
     * the returned JMS Message id that was sent and acked.
     */
    transient public String m_ackedMessageID;
    public Boolean m_wasAck;//indicates if the delievery was succesfull or NOT
    /**
     * the destination name to which we ack back for a successful/or not successful message
     * delivery.
     */
    public String m_destinationNameToBeAcked;

    public JMSAckDataEntry() {
        setFifo(true);
        setNOWriteLeaseMode(true);
    }

   /*public static String[] __getSpaceIndexedFields()
   {
       String[] indexedFields = { "m_destinationNameToBeAcked", "m_producerKey","m_consumerKey" };

       return indexedFields;
   }*/

    /*public String toString()
    {
        return "	JMSAckDataEntry | m_producerKey=  " + m_producerKey +
                                               " | m_consumerKey=  " + m_consumerKey +
                                               " | m_ackedMessageID=  " + m_ackedMessageID +
                                               " | m_ackedExternalEntryUID=  " + m_ackedExternalEntryUID+
                                               " | ackMode=  " + m_ackMode +
                                               " | m_wasAck=  " + m_wasAck +
                                               " | m_destinationNameToBeAcked= " + m_destinationNameToBeAcked;
                                               //" | isFIFO=  " + getFifo();
    }*/
}