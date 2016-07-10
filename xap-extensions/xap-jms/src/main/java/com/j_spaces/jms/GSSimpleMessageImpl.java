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
 * Created on 05/07/2007
 *
 * @author		Shai Wolf
 * Title:       The GigaSpaces Platform
 * Copyright:   Copyright (c) GigaSpaces Team 2004
 * Company:     GigaSpaces Technologies Ltd.
 * @version 	6.0
 */
package com.j_spaces.jms;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 * Implements a simple JMS message.
 *
 * Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 *
 * @author Shai Wolf
 * @version 6.0
 */
public class GSSimpleMessageImpl extends GSMessageImpl implements Message {
    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new instance of <CODE>GSSimpleMessageImpl</CODE>.
     *
     * @throws JMSException if falied to create the message
     */
    public GSSimpleMessageImpl() throws JMSException {
        super();
    }

    /**
     * Instantiates a new instance of <CODE>GSSimpleMessageImpl</CODE>.
     *
     * @param session the session of the message
     * @throws JMSException if falied to create the message
     */
    public GSSimpleMessageImpl(GSSessionImpl session) throws JMSException {
        super(session, GSMessageImpl.SIMPLE);
    }

    /**
     * Creates a duplicate object (deep clone) of the message.
     */
    GSMessageImpl duplicate() throws JMSException {
        GSSimpleMessageImpl dup = new GSSimpleMessageImpl();
        copyTo(dup);
        return dup;
    }
}
