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


package org.openspaces.pu.container.jee;

import org.openspaces.pu.container.spi.ApplicationContextProcessingUnitContainer;
import org.openspaces.pu.service.ServiceDetailsProvider;

import java.util.ArrayList;
import java.util.Collection;

/**
 * An extension to the {@link org.openspaces.pu.container.spi.ApplicationContextProcessingUnitContainer}
 * that can handle JEE processing units.
 *
 * @author kimchy
 */
public abstract class JeeProcessingUnitContainer extends ApplicationContextProcessingUnitContainer implements ServiceDetailsProvider {

    @Override
    public Collection<ServiceDetailsProvider> getServiceDetailsProviders() {
        Collection<ServiceDetailsProvider> result = new ArrayList<ServiceDetailsProvider>();
        result.add(this);
        result.addAll(super.getServiceDetailsProviders());
        return result;
    }
}