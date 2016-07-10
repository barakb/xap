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

package com.gigaspaces.lrmi;

import com.gigaspaces.internal.lrmi.LRMIProxyMonitoringDetailsImpl;


/**
 * Invocation handler used by LRMI framework
 *
 * @author eitany
 * @since 7.1
 */
public interface ILRMIInvocationHandler {
    /**
     * Intercept invocation of a LRMI dynamic proxy
     *
     * @return return monitoring details as Object
     */
    public Object invoke(Object proxy, LRMIMethod lrmiMethod, Object[] args) throws Throwable;

    LRMIProxyMonitoringDetailsImpl getMonitoringDetails();
}
