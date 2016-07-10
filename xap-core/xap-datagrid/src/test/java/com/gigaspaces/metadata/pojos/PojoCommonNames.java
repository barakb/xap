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

package com.gigaspaces.metadata.pojos;

@com.gigaspaces.api.InternalApi
public class PojoCommonNames {
    private String _name;
    private boolean _alive;
    private int _id;
    private String _sillyName;
    private String m_sillyName;

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        this._name = name;
    }

    public boolean isAlive() {
        return _alive;
    }

    public void setAlive(boolean alive) {
        this._alive = alive;
    }

    public int getID() {
        return _id;
    }

    public void setID(int id) {
        this._id = id;
    }

    public String get_sillyName() {
        return _sillyName;
    }

    public void set_sillyName(String name) {
        this._sillyName = name;
    }

    public String getM_sillyName() {
        return m_sillyName;
    }

    public void setM_sillyName(String name) {
        this.m_sillyName = name;
    }

}
