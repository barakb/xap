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

import javax.jms.JMSException;


/**
 * Implements the <code>javax.jms.TopicConnection</code> interface.
 *
 * GigaSpaces implementation of the JMS TopicConnection Inerface.
 * <p><blockquote><pre>
 * It also extends the GSConnectionImpl and overwrites most of its methods.
 * - It holds the list of the sessions that attached to this conn.
 * - It returns a LocalTransaction instance
 * - It starts, stops and closes the Connection
 * </p></blockquote></pre>
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */

public class GSTopicConnectionImpl
        extends GSConnectionImpl {

    /**
     * Constructor: Calling super com.j_spaces.jms.GSConnectionImpl.
     *
     * @param _connFacParent connection factory
     */
    public GSTopicConnectionImpl(GSConnectionFactoryImpl _connFacParent)
            throws JMSException {
        super(_connFacParent);
    }
}//end of class
