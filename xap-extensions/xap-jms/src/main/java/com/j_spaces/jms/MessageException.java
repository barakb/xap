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

import javax.jms.JMSException;

/**
 * A JMSException extension for a JMS Message exception specifics.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class MessageException extends JMSException {
    private static final long serialVersionUID = -3116589292341121954L;

    /**
     *
     */
    public MessageException() {
        super("Message Exception Was Thrown");
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     */
    public MessageException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param cause
     */
    public MessageException(Throwable cause) {
        super(cause.toString());
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     * @param cause
     */
    public MessageException(String message, Throwable cause) {
        super(message, "code -1 TODO");
        // TODO Auto-generated constructor stub
    }

}
