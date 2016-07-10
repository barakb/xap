/*
 * Copyright 2005 Sun Microsystems, Inc.
 * Copyright 2006 GigaSpaces, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jini.rio.core;

import java.io.Serializable;

/**
 * ThresholdValues provides attributes for thresholds which can be applied to a Watch
 */
@com.gigaspaces.api.InternalApi
public class ThresholdValues implements Serializable, Cloneable {
    static final long serialVersionUID = 1L;
    /**
     * Holds value of property highThreshold.
     */
    private double highThreshold = Double.NaN;
    /**
     * Holds value of property lowThreshold.
     */
    private double lowThreshold = Double.NaN;
    /**
     * Holds value of property currentHighThreshold.
     */
    private double currentHighThreshold = Double.NaN;
    /**
     * Holds value of property currentLowThreshold.
     */
    private double currentLowThreshold = Double.NaN;
    /**
     * How many times a Threshold has been breached
     */
    private long breachedCount = 0;
    /**
     * How many times a Threshold has been cleared
     */
    private long clearedCount = 0;

    /**
     * Create a new ThresholdValues
     */
    public ThresholdValues() {
    }

    /**
     * Create a new ThresholdValues
     *
     * @param range Array of double values indicating the range of acceptable lower and upper
     *              thresholds
     */
    public ThresholdValues(double[] range) {
        if (range.length != 2)
            throw new IllegalArgumentException("range must be 2 elements");
        if (range[0] >= range[1])
            throw new IllegalArgumentException("range is not valid");

        this.lowThreshold = range[0];
        this.currentLowThreshold = lowThreshold;
        this.highThreshold = range[1];
        this.currentHighThreshold = highThreshold;

    }

    /**
     * Create a new ThresholdValues
     *
     * @param lowThreshold  The low threshold
     * @param highThreshold The high threshold value
     */
    public ThresholdValues(double lowThreshold, double highThreshold) {
        this.lowThreshold = lowThreshold;
        this.currentLowThreshold = lowThreshold;
        this.highThreshold = highThreshold;
        this.currentHighThreshold = highThreshold;
    }

    /**
     * Getter for property highThreshold.
     *
     * @return Value of property highThreshold.
     */
    public double getHighThreshold() {
        return (highThreshold);
    }

    /**
     * Getter for property lowThreshold.
     *
     * @return Value of property lowThreshold.
     */
    public double getLowThreshold() {
        return (lowThreshold);
    }

    /**
     * Getter for property currentHighThreshold.
     *
     * @return Value of property currentHighThreshold.
     */
    public double getCurrentHighThreshold() {
        return (currentHighThreshold);
    }

    /**
     * Setter for property currentHighThreshold.
     *
     * @param threshold New value of property currentHighThreshold.
     */
    public void setCurrentHighThreshold(double threshold) {
        currentHighThreshold = threshold;
    }

    /**
     * Getter for property currentLowThreshold.
     *
     * @return Value of property currentLowThreshold.
     */
    public double getCurrentLowThreshold() {
        return (currentLowThreshold);
    }

    /**
     * Setter for property currentHighThreshold.
     *
     * @param threshold New value of property currentHighThreshold.
     */
    public void setCurrentLowThreshold(double threshold) {
        currentLowThreshold = threshold;
    }

    /**
     * Reset the currentLowThreshold to tbe original lowThreshold value
     */
    public void resetLowThreshold() {
        this.currentLowThreshold = lowThreshold;
    }

    /**
     * Reset the currentHighThreshold to tbe original highThreshold value
     */
    public void resetHighThreshold() {
        this.currentHighThreshold = highThreshold;
    }

    /**
     * Increments the count of breached thresholds
     */
    public void incThresholdBreachedCount() {
        breachedCount++;
    }

    /**
     * Gets the count of breached thresholds
     */
    public long getThresholdBreachedCount() {
        return (breachedCount);
    }

    /**
     * Increments the count of cleared thresholds
     */
    public void incThresholdClearedCount() {
        clearedCount++;
    }

    /**
     * Gets the count of cleared thresholds
     */
    public long getThresholdClearedCount() {
        return (clearedCount);
    }

    public String toString() {
        return ("low: " + lowThreshold + ", high: " + highThreshold);
    }

    public Object clone() {
        try {
            return (super.clone());
        } catch (CloneNotSupportedException shouldNotHappen) {
            shouldNotHappen.printStackTrace();
        }
        return (null);
    }
}
