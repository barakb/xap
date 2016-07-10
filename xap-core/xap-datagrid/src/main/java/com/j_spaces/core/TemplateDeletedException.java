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


package com.j_spaces.core;

import com.gigaspaces.internal.server.storage.ITemplateHolder;

@com.gigaspaces.api.InternalApi
public class TemplateDeletedException extends Exception {
    private static final long serialVersionUID = 4726214701365053641L;

    public ITemplateHolder m_Template;

    public TemplateDeletedException(ITemplateHolder Template) {
        m_Template = Template;
    }

    @Override
    public String toString() {
        return "Template deleted: " + m_Template;
    }

    /**
     * Override the method to avoid expensive stack build and synchronization, since no one uses it
     * anyway.
     */
    @Override
    public Throwable fillInStackTrace() {
        // TODO Auto-generated method stub
        return null;
    }
}