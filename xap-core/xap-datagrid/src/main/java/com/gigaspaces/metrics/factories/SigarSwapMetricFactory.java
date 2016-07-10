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


package com.gigaspaces.metrics.factories;

import com.gigaspaces.internal.sigar.SigarHolder;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.internal.GaugeContextProvider;
import com.gigaspaces.metrics.internal.InternalGauge;

import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.Swap;

/**
 * @author Niv Ingberg
 * @since 10.1
 */

public class SigarSwapMetricFactory {
    private final SigarSwapWrapper context = new SigarSwapWrapper();

    private static class SigarSwapWrapper extends GaugeContextProvider<Swap> {
        private final Sigar sigar = SigarHolder.getSigar();

        @Override
        protected Swap loadValue() {
            try {
                return sigar.getSwap();
            } catch (SigarException e) {
                throw new RuntimeException("Failed to get swap info from sigar", e);
            }
        }
    }

    public Gauge<Long> createFreeSwapInBytesGauge() {
        return new InternalGauge<Long>(context) {
            @Override
            public Long getValue() throws SigarException {
                return context.get().getFree();
            }
        };
    }

    public Gauge<Long> createUsedSwapInBytesGauge() {
        return new InternalGauge<Long>(context) {
            @Override
            public Long getValue() throws SigarException {
                return context.get().getUsed();
            }
        };
    }

    public Gauge<Double> createUsedSwapInPercentGauge() {
        return new InternalGauge<Double>(context) {
            @Override
            public Double getValue() throws SigarException {
                Swap swap = context.get();
                return calculatePercent(swap.getUsed(), swap.getTotal());
            }
        };
    }
}
