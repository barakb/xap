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

package com.j_spaces.jms;

import com.gigaspaces.internal.version.PlatformVersion;

import java.util.Enumeration;
import java.util.Vector;

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;

/**
 * Implements the <code>javax.jms.ConnectionMetaData</code> interface.
 *
 * The GigaSpaces JMS Connection MetaData class. <br> provides the following: <br> - The supported
 * JMS version <br> - The supported GigaSpaces version <br> - The custom JMSX - provider specific
 * properties.
 *
 * @author Gershon Diner
 * @version 4.0
 */
public class GSConnectionMetaDataImpl implements ConnectionMetaData {
    private static Vector jmsxProperties = new Vector();

    static {
        //TODO currently we do NOT support for the client, JMSXDeliveryCount and JMSXState JMSX props
        //jmsxProperties.add( GSMessageImpl.JMSX_APPID);//set in Send()
        //jmsxProperties.add( GSMessageImpl.JMSX_USERID);//set in Send()
        //jmsxProperties.add( GSMessageImpl.JMSX_PRODUCER_TXID);//set in Send()
        //jmsxProperties.add( GSMessageImpl.JMSX_CONSUMER_TXID);//set in Recieve()
        //jmsxProperties.add( GSMessageImpl.JMSX_RCV_TIMESTEMP);//set in Recieve()
        jmsxProperties.add(GSMessageImpl.JMSX_GROUPID);//set by the client
        jmsxProperties.add(GSMessageImpl.JMSX_GROUPSEQ);//set by the client
    }

    /**
     * Empty Constructor
     */
    public GSConnectionMetaDataImpl() {
        //GSJMSAdmin.say("GSConnectionMetaDataImpl.GSConnectionMetaDataImpl()",Log.D_DEBUG);
    }

    /**
     * API Method
     *
     * @see ConnectionMetaData#getJMSVersion()
     */
    public String getJMSVersion() throws JMSException {
        return "1.1";
    }

    /**
     * API Method
     *
     * @see ConnectionMetaData#getJMSMajorVersion()
     */
    public int getJMSMajorVersion() throws JMSException {
        return 1;
    }

    /**
     * API Method
     *
     * @see ConnectionMetaData#getJMSMinorVersion()
     */
    public int getJMSMinorVersion() throws JMSException {
        return 1;
    }

    /**
     * API Method
     *
     * @see ConnectionMetaData#getJMSProviderName()
     */
    public String getJMSProviderName() throws JMSException {
        return "GigaSpacesJMS";
    }

    /**
     * API Method
     *
     * @see ConnectionMetaData#getProviderVersion()
     */
    public String getProviderVersion() throws JMSException {
        return PlatformVersion.getVersion();
    }

    /**
     * API Method
     *
     * @see ConnectionMetaData#getProviderMajorVersion()
     */
    public int getProviderMajorVersion() throws JMSException {
        return 6;
    }

    /**
     * API Method
     *
     * @see ConnectionMetaData#getProviderMinorVersion()
     */
    public int getProviderMinorVersion() throws JMSException {
        return 0;
    }

    /**
     * API Method
     *
     * @see ConnectionMetaData#getJMSXPropertyNames()
     */
    public Enumeration getJMSXPropertyNames() throws JMSException {
        return jmsxProperties.elements();
    }

}
