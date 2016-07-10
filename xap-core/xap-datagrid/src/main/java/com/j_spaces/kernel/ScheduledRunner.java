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
 * Title:		 ScheduledRunner.java
 * Description: A daemon thread scheduled to run at a fixed interval.	
 * Company:		 GigaSpaces Technologies
 * 
 * @author		 Moran Avigdor
 * @version		 1.0 Jan 31, 2006
 * @since		 5.0EAG Build#1373
 */
package com.j_spaces.kernel;

import com.gigaspaces.internal.utils.concurrent.GSThread;

/**
 * <p> ScheduledRunner is a single daemon thread that wakes up at fixed intervals, to run a single
 * Runnable task. Calling <code>cancel()</code> method will wake up and terminate this daemon
 * thread. <p> Much like {@link java.util.Timer} but without the overhead of a managed queue of
 * tasks. <p>
 * <pre><code>
 * Usage:
 * ScheduledRunner runner =
 * 	new ScheduledRunner( new YourRunnable(), "Runnable Name", 0 /delay/, 1000 /period/);
 *
 * ...
 * runner.reschedule(2000); //reschedule with a new period
 * ...
 * runner.cancel();	//cancel - no longer needed
 * runner = null; 	//help gc
 * </code></pre>
 */
@com.gigaspaces.api.InternalApi
public class ScheduledRunner extends GSThread {
    final private long _delay;
    private long _period;

    /**
     * Allocates a new <code>ScheduledRunner</code> object. This constructor has the same effect as
     * <code>ScheduledRunner(target, </code> <i>name</i><code>)</code> where <i>name</i> is the
     * target's class name.
     *
     * @param target the object whose <code>run</code> method is called.
     * @param delay  delay in milliseconds before task is to be executed.
     * @param period time in milliseconds between successive Runnable target executions.
     * @see java.lang.Thread#Thread(java.lang.Runnable, java.lang.String)
     */
    public ScheduledRunner(Runnable target, long delay, long period) {
        this(target, target.getClass().getName(), delay, period);
    }

    /**
     * Allocates a new <code>ScheduledRunner</code> object. This constructor has the same effect as
     * <code>ScheduledRunner(target)</code> where <i>name</i> is a chosen name for this Runnable
     * target.
     *
     * @param target the object whose <code>run</code> method is called.
     * @param name   the chosen name for this Runnable target.
     * @param period time in milliseconds between successive Runnable target executions.
     * @see java.lang.Thread#Thread(java.lang.Runnable, java.lang.String)
     */
    public ScheduledRunner(Runnable target, String name, long delay, long period) {
        super(target, name);
        super.setDaemon(true);

        _delay = delay;
        _period = period;
        this.start();
    }

    /**
     * Runs the Runnable target's <code>run</code> method after every sleep interval unless
     * <code>cancel</code> method was invoked, or if this thread was Interrupted.
     */
    @Override
    public void run() {
        try {
            if (_delay > 0) {
                fallAsleep(_delay);
            }

            while (!isInterrupted()) {
                super.run();
                fallAsleep();
            }
        } catch (InterruptedException ie) {

            //Restore the interrupted status
            interrupt();
            //fall through
        }
    }

    /**
     * Fall asleep for the specified fixed duration.
     *
     * @param duration to fall asleep for.
     * @throws InterruptedException if another thread interrupted the current thread before or while
     *                              the current thread was waiting for a notification.
     */
    private synchronized final void fallAsleep(long duration)
            throws InterruptedException {
        wait(duration);
    }

    /**
     * Fall asleep for the specified fixed duration (_period).
     *
     * @throws InterruptedException if another thread interrupted the current thread before or while
     *                              the current thread was waiting for a notification.
     */
    private synchronized final void fallAsleep()
            throws InterruptedException {
        wait(_period);
    }

    /**
     * Wakes up this thread while the current thread is waiting for the specified sleep duration.
     * Invoked by <code>cancel</code> method.
     */
    private synchronized final void wakeUp() {
        notify();
    }

    /**
     * Reschedules this Runnable execution for a new specified duration immediately. Much like
     * scheduling with a no delay for a renewal period.
     *
     * If the Runnable target is currently awake, it will fall asleep only after its execution.
     * Otherwise, it will wakeup, run, and fall asleep for the new duration.
     *
     * @param period the new period for this Runnable target.
     */
    public synchronized final void reschedule(long period) {
        _period = period;
        wakeUp();
    }

    /**
     * Cancels all subsequent executions of the Runnable target. If a target is currently running,
     * it will end gracefully.
     */
    public void cancel() {
        interrupt();
    }
}