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

import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;

import net.jini.core.lease.Lease;
import net.jini.lease.LeaseListener;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.Set;

/**
 * this class is used to configure an EventSession. <br> it contains a set of configuration
 * parameters that influence the way event listeners are registered with the space <br> and how
 * event notifications are processed. <br> the names of the parameters that can be used in the
 * properties object or file are: <br> <tt>comType</tt>  - specifies the communication protocol:
 * UNICAST / MULTIPLEX  <br> <tt>batchSize</tt> - the size of the batch used when sending
 * notifications to the client.<br> must be used with <tt>batchTime</tt> <br> <tt>batchTime</tt> -
 * the maximum elapsed time between two batch notifications. <br> must be used with
 * <tt>batchSize</tt> <br> <tt>reliable</tt>  - whether the notification process is reliable. <br>
 * <tt>renew</tt>     - whether to not to automatically renew the lease of the registered listeners.
 * <br> <tt>durable</tt>   - whether the events are durable. <br> <tt>replicateNotifyTemplate</tt> -
 * whether to replicate the registration to other spaces in the cluster
 * <tt>triggerNotifyTemplate</tt> - whether to send notifications from all spaces in the cluster.
 * <tt>guaranteedNotifications</tt> - whether to generate notifications that won't be lost during
 * failover.
 *
 * @author asy ronen
 * @version 1.0
 * @see com.gigaspaces.events.EventSession
 * @since 6.0
 */

public class EventSessionConfig implements Serializable, Textualizable {
    private static final long serialVersionUID = 4035209046639421771L;

    public static final String USE_OLD_LEASE_RENEWAL_MANAGER_PROPERTY = "com.gs.core.notify.old-lease-renewal-manager";

    public static final boolean USE_OLD_GUARANTEED_NOTIFICATIONS = Boolean.getBoolean("com.gs.core.notify.old-guaranteed-notifications");
    public static final boolean USE_OLD_LEASE_RENEWAL_MANAGER = Boolean.getBoolean(USE_OLD_LEASE_RENEWAL_MANAGER_PROPERTY);

    /**
     * Custom Communication type is deprecated since 9.7 - the default is multiplex and there are no
     * benefits for using unicast.
     *
     * @deprecated Since 9.7
     */
    @Deprecated
    public static enum ComType {
        UNICAST, MULTIPLEX
    }

    /**
     * Custom Communication type is deprecated since 9.7 - the default is multiplex and there are no
     * benefits for using unicast.
     *
     * @deprecated Since 9.7
     */
    @Deprecated
    public static final ComType DEFAULT_COM_TYPE = ComType.MULTIPLEX;

    /**
     * @deprecated Since 9.7 - Custom auto-renew id deprecated.
     */
    @Deprecated
    public static final long DEFAULT_RENEW_EXPIRATION = Lease.FOREVER;
    /**
     * @deprecated Since 9.7 - Custom auto-renew id deprecated.
     */
    public static final long DEFAULT_RENEW_DURATION = 20000;
    /**
     * @deprecated Since 9.7 - Custom auto-renew id deprecated.
     */
    public static final long DEFAULT_RENEW_RTT = 10000;

    private static final int DEFAULT_BATCH_SIZE = 0;
    private static final long DEFAULT_BATCH_TIME = 0;
    private static final int DEFAULT_BATCH_PENDING_THRESHOLD = 0;

    public static final int DEFAULT_DURABLE_BATCH_SIZE = 5000;
    public static final int DEFAULT_DURABLE_BATCH_PENDING_THRESHOLD = 100;
    public static final long DEFAULT_DURABLE_BATCH_TIME = 10;

    private ComType comType = DEFAULT_COM_TYPE;
    private boolean fifo = false;
    private boolean batch = false;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private long batchTime = DEFAULT_BATCH_TIME;
    private int batchPendingThreshold = DEFAULT_BATCH_PENDING_THRESHOLD;
    private boolean renew = false;
    private boolean durable = false;
    private Boolean triggerNotifyTemplate;
    private Boolean replicateNotifyTemplate;
    private LeaseListener leaseListener = null;
    private long renewExpiration = DEFAULT_RENEW_EXPIRATION;
    private long renewDuration = DEFAULT_RENEW_DURATION;
    private long renewRTT = DEFAULT_RENEW_RTT;
    private boolean _guaranteedNotifications = false; //default value
    //private boolean notifyPreviousValueOnUpdate = false;

    /**
     * creates EventSessionConfig for farther configuration.
     */
    public EventSessionConfig() {
    }

    /**
     * creates EventSessionConfig and start it's configuration from the Properties given. use
     * properties name as keys for the given Properties.
     *
     * @param props - holds the keys and values.
     * @deprecated since 9.0 use setters instead
     */
    @Deprecated
    public EventSessionConfig(Properties props) {
        loadFromProps(props);
    }

    /**
     * creates EventSessionConfig from a loaded file in the given name. the file should be
     * ".properties" in the config folder.
     *
     * @param schemaName - the file name.
     * @throws IOException - thrown when fail to read from file.
     * @deprecated since 9.0 use setters instead
     */
    @Deprecated
    public EventSessionConfig(String schemaName) throws IOException {
        if (schemaName == null)
            return;

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null)
            cl = getClass().getClassLoader();

        Properties props = new Properties();
        props.load(cl.getResourceAsStream("config/" + schemaName + ".properties"));
        loadFromProps(props);
    }

    private void loadFromProps(Properties props) {
        Set<Object> keys = props.keySet();
        for (Object key : keys) {
            String value = props.getProperty((String) key);

            if (key.equals("comType")) {
                comType = ComType.valueOf(value);
            } else if (key.equals("fifo")) {
                fifo = Boolean.parseBoolean(value);
            } else if (key.equals("batchSize")) {
                batch = true;
                batchSize = Integer.parseInt(value);
            } else if (key.equals("batchTime")) {
                batch = true;
                batchTime = Integer.parseInt(value);
            } else if (key.equals("batchPendingThreshold")) {
                batch = true;
                batchPendingThreshold = Integer.parseInt(value);
            } else if (key.equals("autoRenew")) {
                renew = Boolean.parseBoolean(value);
            } else if (key.equals("durable")) {
                durable = Boolean.parseBoolean(value);
                setDurableNotifications(durable);
            } else if (key.equals("triggerNotifyTemplate")) {
                triggerNotifyTemplate = Boolean.parseBoolean(value);
            } else if (key.equals("replicateNotifyTemplate")) {
                replicateNotifyTemplate = Boolean.parseBoolean(value);
            } else if (key.equals("renewExpiration")) {
                renewExpiration = Long.parseLong(value);
            } else if (key.equals("renewDuration")) {
                renewDuration = Long.parseLong(value);
            } else if (key.equals("renewRTT")) {
                renewRTT = Long.parseLong(value);
            } else if (key.equals("guaranteedNotifications")) {
                _guaranteedNotifications = Boolean.parseBoolean(value);
            }
        }
    }

    /**
     * Custom Communication type is deprecated since 9.7 - the default is multiplex and there are no
     * benefits for using unicast.
     *
     * @deprecated Since 9.7
     */
    @Deprecated
    public ComType getComType() {
        return comType;
    }

    /**
     * Custom Communication type is deprecated since 9.7 - the default is multiplex and there are no
     * benefits for using unicast.
     *
     * @deprecated Since 9.7
     */
    @Deprecated
    public EventSessionConfig setComType(ComType comType) {
        this.comType = comType;
        return this;
    }

    /**
     * Checks if the listener is interested in receiving the previous value when it an update occurs.
     *
     * @return true if the previous value should be sent with the new one.
     */
    /*
    public boolean isNotifyPreviousValueOnUpdate() {
        return notifyPreviousValueOnUpdate;
    }
    */
    /**
     * sets the flag for returning the previous value with the new one upon update notification.
     * @param notifyPreviousValueOnUpdate new value for the flag.
     */
    /*
    public EventSessionConfig setNotifyPreviousValueOnUpdate(boolean notifyPreviousValueOnUpdate) {
        this.notifyPreviousValueOnUpdate = notifyPreviousValueOnUpdate;
        return this;
    }
    */

    /**
     * Checks if the order that is in use with this config is fifo.
     *
     * @return true if notification comes in fifo order.
     */
    public boolean isFifo() {
        return fifo;
    }

    /**
     * Set fifo order for the notifications.
     *
     * @param fifo - true when using fifo order.
     */
    public EventSessionConfig setFifo(boolean fifo) {
        this.fifo = fifo;
        return this;
    }

    /**
     * Checks if using batch notifications.
     *
     * @return <code>true</code> if notification comes in batches both size {@literal >} 0 and time
     * {@literal >} 0 were set.
     * @see #setBatch
     */
    public boolean isBatching() {
        return batch;
    }

    /**
     * set the notifications to come in batches of <tt>size</tt>. notification can not be delay more
     * then <tt>time</tt> ms.
     *
     * @param size - amount of notifications held at server before sending them and the amount of
     *             notification in batch.
     * @param time - maximum time to delay notification in not full batch.
     */
    public void setBatch(int size, long time) {
        setBatch(size, time, size);
    }

    /**
     * set the notifications to come in batches of <tt>size</tt>. notification can not be delay more
     * then <tt>time</tt> ms.
     *
     * @param size             - amount of notification in batch.
     * @param time             - maximum time to delay notification in not full batch.
     * @param pendingThreshold - amount of notifications held at server before sending them.
     */
    public void setBatch(int size, long time, int pendingThreshold) {
        this.batch = size > 0 && time > 0 && pendingThreshold > 0;
        this.batchSize = size;
        this.batchTime = time;
        this.batchPendingThreshold = pendingThreshold;
    }

    /**
     * @return <tt>batchSize</tt> number of notifications in a batch.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * @return <tt>batchTime</tt> - the maximum elapsed time between two batch notifications. <br>
     */
    public long getBatchTime() {
        return batchTime;
    }

    /**
     * return <tt>batchPendingThreshold</tt> - number of notifications held at server before sending
     * them
     */
    public int getBatchPendingThreshold() {
        return batchPendingThreshold;
    }

    public boolean isAutoRenew() {
        return renew;
    }

    /**
     * enable the auto renew of the notify.<BR> use to keep getting events until client fails. same
     * as: {@link #setAutoRenew(boolean, LeaseListener, long, long, long) setAutoRenew(renew,
     * listener, Lease.FOREVER, 20000, 10000)}
     *
     * @param renew    - true when autoRenew needed.
     * @param listener for events when renew fails.
     */
    public EventSessionConfig setAutoRenew(boolean renew, LeaseListener listener) {
        this.renew = renew;
        if (renew) {
            this.leaseListener = listener;
        }
        return this;
    }

    /**
     * with this method the user can configure the renew time duration for the auto renew. Using the
     * renewDuration parameter, you define the period of time that passes between the time a client
     * failure occurred, and the time your notifications stop being sent from the space
     *
     * @param renew           - true when autoRenew needed.
     * @param listener        for events when renew fails.
     * @param renewExpiration the period of time your notifications stop being renewed.
     * @param renewDuration   the period of time that passes between client failure, and the time
     *                        your notifications stop being sent. use more than renewRTT.
     * @param renewRTT        - RoundTripTime - the time that takes to reach the server and return.
     *                        default 10000.
     * @deprecated Since 9.7 - Customizing auto renew is deprecated, use the simple
     * setAutoRenew(boolean, LeaseListener) instead.
     */
    @Deprecated
    public void setAutoRenew(boolean renew, LeaseListener listener,
                             long renewExpiration, long renewDuration, long renewRTT) {
        this.renew = renew;
        if (renew) {
            this.leaseListener = listener;
            this.renewRTT = renewRTT;
            this.renewDuration = renewDuration;
            this.renewExpiration = renewExpiration;
        }
    }

    /**
     * @return the lease listener in case of LRM use. otherwise - returns null.
     */
    public LeaseListener getLeaseListener() {
        return leaseListener;
    }

    /**
     * @return whether to generate notifications that won't be lost during failover and
     * disconnection
     */
    public boolean isDurableNotifications() {
        return durable;
    }

    /**
     * Sets whether to generate notifications that won't be lost during failover and disconnection
     */
    public EventSessionConfig setDurableNotifications(boolean durable) {
        this.durable = durable;

        if (durable) {
            if (batchSize <= 0) {
                batchSize = DEFAULT_DURABLE_BATCH_SIZE;
            }

            if (batchTime <= 0) {
                batchTime = DEFAULT_DURABLE_BATCH_TIME;
            }
            if (batchPendingThreshold <= 0) {
                batchTime = DEFAULT_DURABLE_BATCH_PENDING_THRESHOLD;
            }
            batch = true;
        }
        return this;
    }

    /**
     * Should notify template be triggered on replication event.
     *
     * @return <code>true</code> if trigger is needed on replication event. <code>null</code> is
     * returned when this value was never set.
     */
    public Boolean isTriggerNotifyTemplate() {
        return triggerNotifyTemplate;
    }

    /**
     * @param triggerNotifyTemplate
     */
    public EventSessionConfig setTriggerNotifyTemplate(boolean triggerNotifyTemplate) {
        this.triggerNotifyTemplate = triggerNotifyTemplate;
        return this;
    }

    /**
     * Should this template be replicated.
     *
     * @return <code>true</code> if this template should be replicated. <code>null</code> is
     * returned when this value was not set.
     */
    public Boolean isReplicateNotifyTemplate() {
        return replicateNotifyTemplate;
    }

    /**
     * @param replicateNotifyTemplate
     */
    public EventSessionConfig setReplicateNotifyTemplate(boolean replicateNotifyTemplate) {
        this.replicateNotifyTemplate = replicateNotifyTemplate;
        return this;
    }

    /**
     * @return the renew Duration.
     */
    public long getRenewDuration() {
        return renewDuration;
    }

    /**
     * @return Returns the renewExpiration.
     */
    public long getRenewExpiration() {
        return renewExpiration;
    }

    /**
     * @return Returns the renewRTT.
     */
    public long getRenewRTT() {
        return renewRTT;
    }

    /**
     * @return whether to generate notifications that won't be lost during failover
     */
    public boolean isGuaranteedNotifications() {
        if (USE_OLD_GUARANTEED_NOTIFICATIONS)
            return _guaranteedNotifications;
        else
            return isDurableNotifications();
    }

    /**
     * Sets whether to generate notifications that won't be lost during failover
     *
     * @deprecated Since 9.0 use {@link #setDurableNotifications(boolean)} instead.
     */
    @Deprecated
    public void setGuaranteedNotifications(boolean guaranteedNotifications) {
        if (USE_OLD_GUARANTEED_NOTIFICATIONS)
            _guaranteedNotifications = guaranteedNotifications;
        else
            setDurableNotifications(guaranteedNotifications);

    }

    /**
     * Checks configuration validity
     */
    public void validate() {
        if (USE_OLD_GUARANTEED_NOTIFICATIONS && isGuaranteedNotifications() && isFifo())
            throw new UnsupportedOperationException("Notify delivery in case of a server failure can not be guaranteed for FIFO notifications.");

        if (isDurableNotifications()) {
            if (triggerNotifyTemplate != null && triggerNotifyTemplate)
                throw new UnsupportedOperationException("Notify template cannot be triggered at backup space in durable notifications");

            if (replicateNotifyTemplate != null && !replicateNotifyTemplate)
                throw new UnsupportedOperationException("Notify template is always replicated in durable notifications");
        } else {
            if (isBatching() && getBatchSize() != getBatchPendingThreshold())
                throw new UnsupportedOperationException("Batch pending threshold cannot be different than batch size in non-durable notification");
        }

    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("comType", getComType());
        textualizer.append("fifo", isFifo());
        textualizer.append("durable", isDurableNotifications());
        textualizer.append("guaranteed", isGuaranteedNotifications());
        textualizer.appendIfNotNull("triggerNotifyTemplate", isTriggerNotifyTemplate());
        textualizer.appendIfNotNull("replicateNotifyTemplate", isReplicateNotifyTemplate());
        textualizer.append("batch", isBatching());
        if (isBatching()) {
            textualizer.append("batchSize", getBatchSize());
            textualizer.append("batchTime", getBatchTime());
            textualizer.append("batchPendingThreshold", getBatchPendingThreshold());
        }
        textualizer.append("autoRenew", isAutoRenew());
        if (isAutoRenew()) {
            textualizer.append("leaseListener", getLeaseListener());
            textualizer.append("renewExpiration", getRenewExpiration());
            textualizer.append("renewDuration", getRenewDuration());
            textualizer.append("renewRTT", getRenewRTT());
        }
    }
}
