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


package com.j_spaces.kernel;


/**
 * Defines an interface for a pool of reusable resources. Each resource can used simultaneously by
 * several users and therefore doesn't need to be freed. Suitable for concurrent resources like
 * locks.
 *
 * @author anna
 * @version 1.0
 * @since 6.6
 */
public interface IReusableResourcePool<T> {

    /**
     * @return one of the  resources in the pool
     */
    T getResource();

    /**
     * @return specific resource from the pool
     */
    T getResource(int index);

    /**
     * @return pool size
     */
    int size();
}
