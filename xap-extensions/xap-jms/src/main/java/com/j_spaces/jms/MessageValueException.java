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

/**
 * A JMSException extension for a JMS Message exception specifics. It has been used in cases when an
 * invalid values/types are used whitin JMS Message header/properties/body.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class MessageValueException extends MessageException {
    private static final long serialVersionUID = -7663404974295119328L;

    /**
     *
     */
    public MessageValueException() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     */
    public MessageValueException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param cause
     */
    public MessageValueException(Throwable cause) {
        super(cause);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     * @param cause
     */
    public MessageValueException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

}
