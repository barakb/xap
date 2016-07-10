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

package com.gigaspaces.client.transaction;

import com.gigaspaces.client.transaction.ITransactionManagerProvider.TransactionManagerType;

/**
 * Holds lookup distributed transaction manager configuration properties.
 *
 * @author idan
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class LookupTransactionManagerConfiguration extends TransactionManagerConfiguration {

    private String _lookupTransactionName;
    private String _lookupTransactionGroups;
    private String _lookupTransactionLocators;
    private long _lookupTransactionTimeout;

    public LookupTransactionManagerConfiguration() {
        this(null, null, null, 0);
    }

    public LookupTransactionManagerConfiguration(String lookupTransactionName, String lookupTransactionGroups,
                                                 String lookupTransactionLocators, long lookupTransactionTimeout) {
        super(TransactionManagerType.LOOKUP_DISTRIBUTED);
        _lookupTransactionName = lookupTransactionName;
        _lookupTransactionGroups = lookupTransactionGroups;
        _lookupTransactionLocators = lookupTransactionLocators;
        _lookupTransactionTimeout = lookupTransactionTimeout;
    }

    public String getLookupTransactionName() {
        return _lookupTransactionName;
    }

    public void setLookupTransactionName(String lookupTransactionName) {
        _lookupTransactionName = lookupTransactionName;
    }

    public String getLookupTransactionGroups() {
        return _lookupTransactionGroups;
    }

    public void setLookupTransactionGroups(String lookupTransactionGroups) {
        _lookupTransactionGroups = lookupTransactionGroups;
    }

    public String getLookupTransactionLocators() {
        return _lookupTransactionLocators;
    }

    public void setLookupTransactionLocators(String lookupTransactionLocators) {
        _lookupTransactionLocators = lookupTransactionLocators;
    }

    public long getLookupTransactionTimeout() {
        return _lookupTransactionTimeout;
    }

    public void setLookupTransactionTimeout(long lookupTransactionTimeout) {
        _lookupTransactionTimeout = lookupTransactionTimeout;
    }


}
