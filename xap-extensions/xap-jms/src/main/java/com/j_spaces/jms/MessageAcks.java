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
 * Created on 24/05/2004
 *
 * @author		Gershon Diner
 * Title:        	The GigaSpaces Platform
 * Copyright:   Copyright (c) GigaSpaces Team 2004
 * Company:    GigaSpaces Technologies Ltd.
 * @version 	 4.0
 */
package com.j_spaces.jms;

import java.util.Vector;

/**
 * A <code>MessageAcks</code> instance holds the Ack objects. Each (an inner class) holds req data
 * for each Ack, e.g. acked ExternalEntry UID (space ExternalEntries UIDs) on a queue or on a proxy
 * subscription, message producer and message consumer keys and the targeted destination name to
 * which the ack of a succesfull or not-succesfull message deleivery should return.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class MessageAcks {
    /**
     * The vector Ack objects (an inner class that holds req data for each Ack).
     */
    private Vector acksVec;
    /**
     * <code>true</code> if the messages to acknowledge are on a queue.
     */
    private volatile boolean queueMode;

    /**
     * the destination name to which we ack back for a sussesfull/or not sucssesfull message
     * deleivery.
     */
    private String destinationNameToBeAcked;

    /**
     * Constructs a <code>MessageAcks</code> instance.
     *
     * @param queueMode <code>true</code> for queue messages.
     */
    MessageAcks(boolean queueMode, String destNameToBeAcked) {
        this.queueMode = queueMode;
        acksVec = new Vector();
        this.destinationNameToBeAcked = destNameToBeAcked;
    }

    /**
     * Creates new Ack obj (inner class), with all the req info and adds it to the acks vec.
     */
    void addAck(String producerKey,
                String consumerKey,
                      /*String ackedExternalEntryUID,*/
                String ackedMessageID,
                String destNameToBeAcked) {
        acksVec.add(new Ack(producerKey, consumerKey/*, ackedExternalEntryUID*/, ackedMessageID, destNameToBeAcked));
    }

    /**
     * Returns the vector of Ack objects.
     */
    public Vector getAcksVec() {
        return acksVec;
    }

    /**
     * Returns <code>true</code> if the messages to acknowledge are on a queue.
     */
    boolean getQueueMode() {
        return queueMode;
    }

    /**
     * return the destination name to which we ack back for a sussesfull/or not sucssesfull message
     * deleivery.
     *
     * @return destinationNameToBeAcked
     */
    public String getTargetName() {
        return this.destinationNameToBeAcked;
    }

    /**
     * @param targetName
     */
    public void setTargetName(String targetName) {
        this.destinationNameToBeAcked = targetName;
    }

    /**
     * The Ack obj, is an inner class wrapper that holds all the required information for a
     * sucessfull acknoledge of a JMSMessage.
     */
    static class Ack {
        String producerKey;
        String consumerKey;
        /*String ackedExternalEntryUID;*/
        String ackedMessageID;
        String destNameToBeAcked;

        /**
         * Const for the Inner-Class
         */
        Ack(String producerKey,
            String consumerKey,
                /*String ackedExternalEntryUID,*/
            String ackedMessageID,
            String destNameToBeAcked) {
            this.producerKey = producerKey;
            this.consumerKey = consumerKey;
			 /*this.ackedExternalEntryUID = ackedExternalEntryUID;*/
            this.ackedMessageID = ackedMessageID;
            this.destNameToBeAcked = destNameToBeAcked;
        }
        /**
         * @return
         *//*
		public String getAckedExternalEntryUID()
		{
			return ackedExternalEntryUID;
		}*/

        /**
         * @return
         */
        public String getAckedMessageID() {
            return ackedMessageID;
        }

        /**
         * @return
         */
        public String getConsumerKey() {
            return consumerKey;
        }

        /**
         * @return
         */
        public String getProducerKey() {
            return producerKey;
        }

        /**
         * @return
         */
        public String getDestNameToBeAcked() {
            return destNameToBeAcked;
        }
    }//end of inner class

}//end of class
