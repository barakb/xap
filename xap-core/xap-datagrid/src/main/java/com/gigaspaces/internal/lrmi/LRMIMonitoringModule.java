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

package com.gigaspaces.internal.lrmi;

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.lrmi.LRMIMethodMonitoringDetails;
import com.gigaspaces.lrmi.nio.Pivot;
import com.gigaspaces.lrmi.nio.Reader;
import com.gigaspaces.lrmi.nio.Writer;

import java.util.Map;
import java.util.Map.Entry;

/**
 * A monitoring module use to monitor activity of remote lrmi method invocations.
 *
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class LRMIMonitoringModule {
    final private Map<String, LRMIMethodMonitoringDetailsImpl> _monitoringDetails = CollectionsFactory.getInstance().createMap();

    private long _lastMonitoredGeneratedTraffic;
    private long _lastMonitoredReceivedTraffic;

    public void monitorActivity(String monitoringId, Writer writer, Reader reader) {
        if (!Pivot.isMonitorActivity() || monitoringId == null || writer == null || reader == null)
            return;

        LRMIMethodMonitoringDetailsImpl methodMonitoringDetails = _monitoringDetails.get(monitoringId);
        if (methodMonitoringDetails == null) {
            methodMonitoringDetails = new LRMIMethodMonitoringDetailsImpl();
            _monitoringDetails.put(monitoringId, methodMonitoringDetails);
        }
        long generatedTraffic = writer.getGeneratedTraffic();
        //this is not volatile, so we may have a non updates result
        long thisInvocationGeneratedTraffic = Math.max(0, generatedTraffic - _lastMonitoredGeneratedTraffic);
        _lastMonitoredGeneratedTraffic = generatedTraffic;
        long receivedTraffic = reader.getReceivedTraffic();
        //this is not volatile, so we may have a non updates result
        long thisInvocationReceivedTraffic = Math.max(0, receivedTraffic - _lastMonitoredReceivedTraffic);
        _lastMonitoredReceivedTraffic = receivedTraffic;
        methodMonitoringDetails.addTrackingDetails(thisInvocationGeneratedTraffic, thisInvocationReceivedTraffic);
    }

    public void addMonitoringActivity(Map<String, LRMIMethodMonitoringDetails> trackingDetails) {
        try {
            for (Entry<String, LRMIMethodMonitoringDetailsImpl> methodTrackingDetailsEntry : _monitoringDetails.entrySet()) {
                LRMIMethodMonitoringDetailsImpl methodTrackingDetails = (LRMIMethodMonitoringDetailsImpl) trackingDetails.get(methodTrackingDetailsEntry.getKey());
                if (methodTrackingDetails == null) {
                    methodTrackingDetails = new LRMIMethodMonitoringDetailsImpl();
                    trackingDetails.put(methodTrackingDetailsEntry.getKey(), methodTrackingDetails);
                }
                methodTrackingDetails.addTrackingDetails(methodTrackingDetailsEntry.getValue());
            }
        } catch (Exception e) {
            //The trove map is not thread safe, however we rarely put new methods into the map and rarely read tracking details so we don't
            //want to guard this structure with lock in order not to reduce performance of tracking operations. 
            //We may read the map while it is being updated (the map may only be resized when new methods are first invoked).
            //If we encounter such exception we will ignore the remaining tracking details of this specific connection on this occurance
        }
    }
}
