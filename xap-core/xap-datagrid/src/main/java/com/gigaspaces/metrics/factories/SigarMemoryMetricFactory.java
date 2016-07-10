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

import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

/**
 * @author Niv Ingberg
 * @since 10.1
 */

public class SigarMemoryMetricFactory {
    private final SigarMemWrapper context = new SigarMemWrapper();
    private final boolean _isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");

    private static class SigarMemWrapper extends GaugeContextProvider<Mem> {
        private final Sigar sigar = SigarHolder.getSigar();

        @Override
        protected Mem loadValue() {
            try {
                return sigar.getMem();
            } catch (SigarException e) {
                throw new RuntimeException("Failed to get memory info from sigar", e);
            }
        }
    }

    public Gauge<Long> createFreeMemoryInBytesGauge() {
        return new InternalGauge<Long>(context) {
            @Override
            public Long getValue() throws SigarException {
                return context.get().getFree();
            }
        };
    }

    public Gauge<Long> createActualFreeMemoryInBytesGauge() {
        return new InternalGauge<Long>(context) {
            @Override
            public Long getValue() throws SigarException {
                return context.get().getActualFree();
            }
        };
    }

    public Gauge<Long> createUsedMemoryInBytesGauge() {
        return new InternalGauge<Long>(context) {
            @Override
            public Long getValue() throws SigarException {
                return context.get().getUsed();
            }
        };
    }

    public Gauge<Long> createActualUsedMemoryInBytesGauge() {
        // See GS-11887
        if (_isWindows) {
            return new InternalGauge<Long>(context) {
                @Override
                public Long getValue() throws SigarException {
                    final Mem mem = context.get();
                    return mem.getTotal() - mem.getFree();
                }
            };
        }
        return new InternalGauge<Long>(context) {
            @Override
            public Long getValue() throws SigarException {
                return context.get().getActualUsed();
            }
        };
    }

    public Gauge<Double> createUsedMemoryInPercentGauge() {
        if (_isWindows) {
            return new InternalGauge<Double>(context) {
                @Override
                public Double getValue() throws SigarException {
                    final Mem mem = context.get();
                    return calculatePercent(mem.getTotal() - mem.getFree(), mem.getTotal());
                }
            };
        }
        return new InternalGauge<Double>(context) {
            @Override
            public Double getValue() throws SigarException {
                return validate(context.get().getUsedPercent());
            }
        };
    }
}
