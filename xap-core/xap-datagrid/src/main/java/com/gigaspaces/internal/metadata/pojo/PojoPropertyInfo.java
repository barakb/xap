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

package com.gigaspaces.internal.metadata.pojo;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class PojoPropertyInfo {
    private final String _name;
    private Class<?> _type;
    private Method _getterMethod;
    private Method _setterMethod;
    private final List<Method> _methods;

    public PojoPropertyInfo(String name) {
        this._name = name;
        this._methods = new ArrayList<Method>();
    }

    public String getName() {
        return _name;
    }

    public Class<?> getType() {
        return _type;
    }

    private void setType(Class<?> type) {
        this._type = type;
    }

    public Method getGetterMethod() {
        return _getterMethod;
    }

    private void setGetterMethod(Method method) {
        _getterMethod = method;
    }

    public Method getSetterMethod() {
        return _setterMethod;
    }

    private void setSetterMethod(Method method) {
        _setterMethod = method;
    }

    public List<Method> getMethods() {
        return _methods;
    }

    public void calculateAccessors() {
        Method getMethod = null;
        Method isMethod = null;
        List<Method> setMethods = new ArrayList<Method>();
        for (Method method : _methods) {
            if (method.getParameterTypes().length == 1)
                setMethods.add(method);
            else if (method.getName().startsWith(PojoTypeInfo.PREFIX_IS)) {
                if (isMethod == null)
                    isMethod = method;
            } else {
                if (getMethod == null)
                    getMethod = method;
            }
        }

        if (isMethod != null && getMethod != null) {
            setSetterMethod(findMatchingSetter(setMethods, isMethod));
            if (_setterMethod != null)
                setGetterMethod(isMethod);
            else {
                setSetterMethod(findMatchingSetter(setMethods, getMethod));
                if (_setterMethod != null)
                    setGetterMethod(getMethod);
            }
        } else {
            setGetterMethod(isMethod != null ? isMethod : getMethod);
            if (_getterMethod != null)
                setSetterMethod(findMatchingSetter(setMethods, _getterMethod));
            else if (setMethods.size() == 1)
                setSetterMethod(setMethods.get(0));
            else
                _setterMethod = null;
        }

        if (_getterMethod != null)
            setType(_getterMethod.getReturnType());
        else if (_setterMethod != null)
            setType(_setterMethod.getParameterTypes()[0]);
        else
            setType(null);
    }

    private static Method findMatchingSetter(List<Method> setters, Method getter) {
        Class<?> type = getter.getReturnType();

        for (Method method : setters)
            if (method.getParameterTypes()[0].equals(type))
                return method;

        return null;
    }
}
