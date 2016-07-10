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

package com.j_spaces.jdbc.parser;

/**
 * node holding a single item inside contains, or a hole expression pointed by value
 *
 * @author Yechiel Fefer
 * @since 9.6
 */

@com.gigaspaces.api.InternalApi
public class ContainsItemNode extends ContainsNode {

    public ContainsItemNode() {
        super();
    }

    public ContainsItemNode(ColumnNode columnNode, ExpNode operatorNode, short templateMatchCode) {
        super(columnNode, operatorNode, templateMatchCode);
    }

    /*
     * returns true if its a contains-item node 
     */
    @Override
    public boolean isContainsItemNode() {
        return true;
    }

    @Override
    public Object clone() {
        ContainsItemNode cloned = (ContainsItemNode) super.clone();
        return cloned;
    }

    @Override
    public ExpNode newInstance() {
        return new ContainsItemNode();
    }


}
