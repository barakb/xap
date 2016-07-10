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

import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.QueryTemplateBuilder;

import java.sql.SQLException;

/**
 * This is the IS NULL operator Node. it can also represent an IS NOT NULL
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class IsNullNode extends ExpNode {

    private boolean isNot = false;

    public IsNullNode(ExpNode leftChild, ExpNode rightChild) {
        super(leftChild, rightChild);
    }

    /**
     *
     */
    public IsNullNode() {
    }

    @Override
    public boolean isValidCompare(Object ob1, Object ob2) throws ClassCastException {
        return ((ob1 == null && !isNot) || (ob1 != null && isNot));
    }


    /**
     * @param isNot if true, this is a IS NOT NULL. otherwise it's IS NULL
     */
    public void setNot(boolean isNot) {
        this.isNot = isNot;
    }

    @Override
    public boolean isJoined() {
        return false;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.parser.ExpNode#newInstance()
     */
    @Override
    public ExpNode newInstance() {
        IsNullNode node = new IsNullNode();

        node.setNot(isNot);

        return node;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.parser.ExpNode#accept(com.j_spaces.jdbc.builder.QueryTemplateBuilder, com.j_spaces.core.client.BasicTypeInfo)
     */
    @Override
    public void accept(QueryTemplateBuilder builder)
            throws SQLException {
        builder.build(this, isNot ? TemplateMatchCodes.NOT_NULL : TemplateMatchCodes.IS_NULL);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return leftChild.toString() + (isNot ? " is not null " : " is null");
    }
}
