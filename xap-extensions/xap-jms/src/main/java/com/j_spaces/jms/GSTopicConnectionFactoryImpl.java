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
 * @author Gershon Diner 
 * Title: The GigaSpaces Platform 
 * Copyright: Copyright (c)GigaSpaces Team 2004 
 * Company: GigaSpaces Technologies Ltd.
 * 
 * @version 4.0
 */
package com.j_spaces.jms;

import com.j_spaces.core.IJSpace;
import com.j_spaces.jms.utils.IMessageConverter;

import java.io.Serializable;
import java.rmi.Remote;

import javax.jms.JMSException;

/**
 * GigaSpaces implementation of the <code>javax.jms.TopicConnectionFactory</code> interface. This
 * class is responsible for finding a GigaSpace IJSpace using the SpaceFinder and a url. It also
 * creates TransactionManager and ParserManager instances.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class GSTopicConnectionFactoryImpl extends GSConnectionFactoryImpl
        implements Serializable, Remote {

    /** */
    private static final long serialVersionUID = 1L;


    /**
     * Constructs an <code>GSTopicConnectionFactoryImpl</code>.
     */
    public GSTopicConnectionFactoryImpl(IJSpace space, IMessageConverter messageConverter)
            throws JMSException {
        super(space, messageConverter);
    }


    /**
     * Constructs a default <code>GSTopicConnectionFactoryImpl</code>.
     */
    public GSTopicConnectionFactoryImpl() throws JMSException {
        this(null, null);
    }
}//end of class
