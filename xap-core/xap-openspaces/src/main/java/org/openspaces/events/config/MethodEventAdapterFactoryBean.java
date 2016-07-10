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


package org.openspaces.events.config;

import org.openspaces.events.adapter.AbstractResultEventListenerAdapter;
import org.openspaces.events.adapter.MethodEventListenerAdapter;

/**
 * @author kimchy
 */
public class MethodEventAdapterFactoryBean extends AbstractResultEventAdapterFactoryBean {

    private Object delegate;

    private String methodName;

    public void setDelegate(Object delegate) {
        this.delegate = delegate;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    protected AbstractResultEventListenerAdapter createAdapter() {
        MethodEventListenerAdapter adapter = new MethodEventListenerAdapter();
        adapter.setDelegate(delegate);
        adapter.setMethodName(methodName);
        return adapter;
    }
}
