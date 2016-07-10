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

package com.gigaspaces.internal.utils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class TreeNode<T> {
    private T _data;
    private final TreeNode<T> _parent;
    private final List<TreeNode<T>> _children;

    public TreeNode(T data, TreeNode<T> parent) {
        this._data = data;
        this._parent = parent;
        this._children = new ArrayList<TreeNode<T>>();
        // If parent is not null, add this new node to the parent's children:
        if (parent != null)
            parent._children.add(this);

    }

    public T getData() {
        return _data;
    }

    public void setData(T data) {
        this._data = data;
    }

    public TreeNode<T> getParent() {
        return _parent;
    }

    public List<TreeNode<T>> getChildren() {
        return _children;
    }

    public List<T> buildParentHierarchy() {
        List<T> result = new ArrayList<T>();

        for (TreeNode<T> node = this; node != null; node = node._parent)
            result.add(node._data);

        return result;
    }

    public List<T> buildBFS() {
        List<T> result = new ArrayList<T>();
        LinkedList<TreeNode<T>> queue = new LinkedList<TreeNode<T>>();

        // first add the source
        result.add(_data);
        queue.addFirst(this);
        while (!queue.isEmpty()) {
            TreeNode<T> currentNode = queue.getFirst();
            for (TreeNode<T> childNode : currentNode._children) {
                result.add(childNode._data);
                queue.addLast(childNode);
            }
            queue.removeFirst();
        }

        return result;
    }
}
