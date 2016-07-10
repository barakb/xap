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

package org.openspaces.pu.container.jee.stats;

import org.openspaces.pu.service.PlainServiceMonitors;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Statistics monitor information for JEE servlet requests.
 *
 * @author kimchy
 */
public class WebRequestsServiceMonitors extends PlainServiceMonitors {

    private static final long serialVersionUID = 1345686383755041069L;

    public static class Attributes {
        public static final String TOTAL = "total";
        public static final String ACTIVE = "active";
        public static final String TOTAL_DURATION = "duration-total";
        public static final String REQUESTS_THROUGHPUT = "requests-throughput";
        public static final String AVERAGE_REQUESTS_LATENCY = "average-requests-latency";
    }

    public WebRequestsServiceMonitors() {
    }

    public WebRequestsServiceMonitors(String id, long requests, long requestsActive, long requestsDurationTotal) {
        super(id);
        getMonitors().put(Attributes.TOTAL, requests);
        getMonitors().put(Attributes.ACTIVE, requestsActive);
        getMonitors().put(Attributes.TOTAL_DURATION, requestsDurationTotal);
        getMonitors().put(Attributes.REQUESTS_THROUGHPUT, -1f);
        getMonitors().put(Attributes.AVERAGE_REQUESTS_LATENCY, -1f);
    }

    public void setPrevious(WebRequestsServiceMonitors previous, long timeout) {

        float requestsThroughput = -1;
        float averageRequetsLatency = -1;

        if (previous != null) {
            //calculate requests throughput
            requestsThroughput =
                    1000 * ((float) (getTotal() - previous.getTotal())) / timeout;

            //calculate average requests latency
            long requestsDelta = getTotal() - previous.getTotal();
            long durationDelta = getTotalDuration() - previous.getTotalDuration();
            if (durationDelta != 0) {
                averageRequetsLatency = (float) requestsDelta / durationDelta;
            }
        }

        getMonitors().put(Attributes.REQUESTS_THROUGHPUT, requestsThroughput);
        getMonitors().put(Attributes.AVERAGE_REQUESTS_LATENCY, averageRequetsLatency);
    }

    /**
     * Returns the total number of requests processed.
     */
    public long getTotal() {
        return (Long) getMonitors().get(Attributes.TOTAL);
    }

    /**
     * Returns the current number of requests that are active.
     */
    public long getActive() {
        return (Long) getMonitors().get(Attributes.ACTIVE);
    }

    /**
     * Returns the total time (in milliseconds) that it took to process requests.
     */
    public long getTotalDuration() {
        return (Long) getMonitors().get(Attributes.TOTAL_DURATION);
    }

    /**
     * Returns the web requests throughput ( requests/second )
     */
    public float getRequestsThroughput() {
        return (Float) getMonitors().get(Attributes.REQUESTS_THROUGHPUT);
    }

    /**
     * Returns the average requests latency ( ms )
     */
    public float getAverageRequestsLatency() {
        return (Float) getMonitors().get(Attributes.AVERAGE_REQUESTS_LATENCY);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }
}
