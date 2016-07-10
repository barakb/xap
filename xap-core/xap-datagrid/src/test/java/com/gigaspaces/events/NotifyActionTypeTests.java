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

package com.gigaspaces.events;

import org.junit.Assert;
import org.junit.Test;

@com.gigaspaces.api.InternalApi
public class NotifyActionTypeTests {
    @Test
    public void testToString() {
        // Test none:
        Assert.assertEquals("NOTIFY_NONE", NotifyActionType.NOTIFY_NONE.toString());
        // Test single modifiers:
        Assert.assertEquals("NOTIFY_WRITE", NotifyActionType.NOTIFY_WRITE.toString());
        Assert.assertEquals("NOTIFY_UPDATE", NotifyActionType.NOTIFY_UPDATE.toString());
        Assert.assertEquals("NOTIFY_TAKE", NotifyActionType.NOTIFY_TAKE.toString());
        Assert.assertEquals("NOTIFY_LEASE_EXPIRATION", NotifyActionType.NOTIFY_LEASE_EXPIRATION.toString());
        Assert.assertEquals("NOTIFY_UNMATCHED", NotifyActionType.NOTIFY_UNMATCHED.toString());
        Assert.assertEquals("NOTIFY_MATCHED_UPDATE", NotifyActionType.NOTIFY_MATCHED_UPDATE.toString());
        Assert.assertEquals("NOTIFY_REMATCHED_UPDATE", NotifyActionType.NOTIFY_REMATCHED_UPDATE.toString());
        // Test composite modifiers
        Assert.assertEquals("NOTIFY_WRITE|NOTIFY_UPDATE",
                NotifyActionType.NOTIFY_WRITE_OR_UPDATE.toString());
        Assert.assertEquals("NOTIFY_WRITE|NOTIFY_UPDATE",
                NotifyActionType.NOTIFY_WRITE.or(NotifyActionType.NOTIFY_UPDATE).toString());
        Assert.assertEquals("NOTIFY_WRITE|NOTIFY_UPDATE",
                NotifyActionType.NOTIFY_UPDATE.or(NotifyActionType.NOTIFY_WRITE).toString());
        Assert.assertEquals("NOTIFY_WRITE|NOTIFY_UPDATE|NOTIFY_TAKE|NOTIFY_LEASE_EXPIRATION",
                NotifyActionType.NOTIFY_ALL.toString());
    }
}
