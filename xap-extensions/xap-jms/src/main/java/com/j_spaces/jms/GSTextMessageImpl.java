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
 * @author		 	 Gershon Diner
 * Title:        		 The GigaSpaces Platform
 * Copyright:    	 Copyright (c) GigaSpaces Team 2004
 * Company:      	 GigaSpaces Technologies Ltd.
 * @version 	 	 4.0
 */
package com.j_spaces.jms;


import javax.jms.JMSException;
import javax.jms.TextMessage;


/**
 * Implements the <code>javax.jms.TextMessage</code> interface.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class GSTextMessageImpl extends GSMessageImpl implements TextMessage {

    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new <code>TextMessage</code> instance.
     *
     * @throws JMSException if falied to create the message
     */
    public GSTextMessageImpl() throws JMSException {
        super();
    }


    /**
     * Instantiates a new <code>TextMessage</code> instance.
     *
     * @param session the session of the message
     * @param text    the String content of the message
     * @throws JMSException if falied to create the message
     */
    public GSTextMessageImpl(GSSessionImpl session, String text) throws JMSException {
        super(session, TEXT);
        Body = text;
    }

    /**
     * Instantiates a new <code>TextMessage</code> instance.
     *
     * @param session the session of the message
     * @throws JMSException if falied to create the message
     */
    public GSTextMessageImpl(GSSessionImpl session) throws JMSException {
        this(session, null);
    }


    /**
     * @see TextMessage#setText(String)
     */
    public void setText(String string) throws JMSException {
        checkBodyReadOnly();
        Body = string;
    }


    /**
     * @see TextMessage#getText()
     */
    public String getText() throws JMSException {
        return Body != null ? (String) Body : null;
    }


    /**
     * Creates a copy of this message.
     *
     * @return the copy of this message.
     */
    GSMessageImpl duplicate() throws JMSException {
        GSTextMessageImpl dup = new GSTextMessageImpl();
        copyTo(dup);
        return dup;
    }

    /**
     * Returns a clone of the body.
     *
     * @return a clone of the body.
     */
    protected Object cloneBody() {
        return Body;
    }

}
