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


package org.openspaces.core.space.filter;

import com.j_spaces.core.filters.FilterProvider;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>A base class for filter adapters that delegate the invocation of filter operation and
 * lifecycle methods to another class. The delegate invocation is done using {@link
 * org.openspaces.core.space.filter.FilterOperationDelegate FilterOperationDelegate}.
 *
 * <p>Subclasses should implement three methods. The first, {@link #doGetInvokerLookup()} provides a
 * map of operation per {@link org.openspaces.core.space.filter.FilterOperationDelegateInvoker
 * FilterOperationDelegateInvoker}. The other two provide filter lifecycle methods {@link
 * #doGetInitMethod()} and {@link #doGetCloseMethod()}.
 *
 * @author kimchy
 * @see org.openspaces.core.space.filter.FilterOperationDelegate
 */
public abstract class AbstractFilterProviderAdapterFactoryBean extends AbstractFilterProviderFactoryBean {

    /**
     * <p>Constructs a new {@link com.j_spaces.core.filters.FilterProvider FilterProvider} using
     * {@link org.openspaces.core.space.filter.FilterOperationDelegate FilterOperationDelegate} as
     * the <code>ISpaceFilter</code> implementation.
     *
     * <p>Subclasses should provide the main Map of operation per {@link
     * org.openspaces.core.space.filter.FilterOperationDelegateInvoker
     * FilterOperationDelegateInvoker} which is used to initialize the {@link
     * org.openspaces.core.space.filter.FilterOperationDelegate FilterOperationDelegate}.
     */
    @Override
    protected FilterProvider doGetFilterProvider() throws IllegalArgumentException {
        Map<Integer, FilterOperationDelegateInvoker> invokerLookup = doGetInvokerLookup();
        if (invokerLookup.size() == 0) {
            throw new IllegalArgumentException("No invoker found in filter [" + getFilter() + "]");
        }
        FilterOperationDelegate delegate = new FilterOperationDelegate(getFilter(), invokerLookup);
        delegate.setInitMethod(doGetInitMethod());
        delegate.setCloseMethod(doGetCloseMethod());

        FilterProvider filterProvider = new FilterProvider(getBeanName(), delegate);

        // set up operation codes
        List<Integer> operationCodes = new ArrayList<Integer>();
        for (FilterOperationDelegateInvoker invoker : invokerLookup.values()) {
            operationCodes.add(invoker.getOperationCode());
        }
        int[] opCodes = new int[operationCodes.size()];
        for (int i = 0; i < opCodes.length; i++) {
            opCodes[i] = operationCodes.get(i);
        }
        filterProvider.setOpCodes(opCodes);
        return filterProvider;
    }

    /**
     * Helper method for basclasses that add an invoker to the lookup map. Performs validation that
     * no other invoker is bounded to the operation code.
     */
    protected void addInvoker(Map<Integer, FilterOperationDelegateInvoker> invokerLookup, Method method, int operationCode) throws IllegalArgumentException {
        FilterOperationDelegateInvoker invoker = invokerLookup.get(operationCode);
        if (invoker != null) {
            throw new IllegalArgumentException("Filter adapter only allows for a single method for each operation. " +
                    "operation [" + operationCode + "] has method [" + invoker.getProcessMethod().getName() + "] and method [" +
                    method.getName() + "]");
        }
        // TODO add parameter validation
        invokerLookup.put(operationCode, new FilterOperationDelegateInvoker(operationCode, method));
    }

    /**
     * Responsible for returning a lookup map of operation code to invoker.
     */
    protected abstract Map<Integer, FilterOperationDelegateInvoker> doGetInvokerLookup();

    /**
     * Retruns the filter lifecycle init method delegate. Can be <code>null</code>.
     */
    protected abstract Method doGetInitMethod();

    /**
     * Retruns the filter lifecycle close method delegate. Can be <code>null</code>.
     */
    protected abstract Method doGetCloseMethod();
}
