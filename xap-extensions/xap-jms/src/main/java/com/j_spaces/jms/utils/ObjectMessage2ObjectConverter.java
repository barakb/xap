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

package com.j_spaces.jms.utils;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;

/**
 * This converter converts JMS ObjectMessages to their containing Object.
 */
public class ObjectMessage2ObjectConverter implements IMessageConverter {
    /**
     * If msg is a JMS ObjectMessage, it returns the containing POJO. Otherwise, it returns the same
     * argument object.
     *
     * @param msg the JMS message to convert
     * @return the convertion result
     */
    public Object toObject(Message msg) throws JMSException {
        if (msg != null && msg instanceof ObjectMessage) {
            return ((ObjectMessage) msg).getObject();
        }
        return msg;
    }
}
