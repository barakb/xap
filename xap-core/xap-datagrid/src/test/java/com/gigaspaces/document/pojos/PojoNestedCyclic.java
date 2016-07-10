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

package com.gigaspaces.document.pojos;

import com.gigaspaces.document.SpaceDocument;

import junit.framework.Assert;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class PojoNestedCyclic {
    Node root;
    int totalLeaves;
    Timestamp time;

    public PojoNestedCyclic() {
    }

    public Node getRoot() {
        return root;
    }

    public PojoNestedCyclic setRoot(Node root) {
        this.root = root;
        return this;
    }

    public int getTotalLeaves() {
        return totalLeaves;
    }

    public PojoNestedCyclic setTotalLeaves(int totalLeaves) {
        this.totalLeaves = totalLeaves;
        return this;
    }

    public Timestamp getTime() {
        return time;
    }

    public PojoNestedCyclic setTime(Timestamp time) {
        this.time = time;
        return this;
    }

    public static PojoNestedCyclic createCyclicTree() {
        Node root = new Node().setId("root");
        Node child = new Node().setId("child").setFather(root);
        root.setFather(child);

        return new PojoNestedCyclic().setRoot(root).setTotalLeaves(1).setTime(new Timestamp(1000));

    }

    public static PojoNestedCyclic createMixedTree() {
        Node root = new Node().setId("root");
        Node child1 = new Node().setId("child1").setFather(root);
        Leaf leaf1 = new Leaf();
        leaf1.setId("leaf1").setFather(root);
        Leaf leaf2 = new Leaf();
        leaf2.setId("leaf2").setFather(child1);
        leaf1.setRightSibling(leaf2).setData("leaf1Data");
        leaf2.setLeftSibling(leaf1).setData("leaf2Data");

        List<Node> rootChildren = new LinkedList<PojoNestedCyclic.Node>();
        rootChildren.add(leaf1);
        rootChildren.add(child1);
        root.setChildren(rootChildren);

        LinkedList<Node> child1Children = new LinkedList<PojoNestedCyclic.Node>();
        child1Children.add(leaf2);
        child1.setChildren(child1Children);

        return new PojoNestedCyclic()
                .setTotalLeaves(2)
                .setRoot(root)
                .setTime(new Timestamp(7000));
    }

    public static void validateCyclicTree(SpaceDocument document) {
        SpaceDocument resultRoot = (SpaceDocument) document.getProperty("root");
        Assert.assertNotNull(resultRoot);
        Assert.assertTrue(resultRoot instanceof SpaceDocument);

        List<SpaceDocument> resultRootChildren = (List<SpaceDocument>) resultRoot.getProperty("children");
        Assert.assertNotNull(resultRootChildren);
        Assert.assertTrue(resultRootChildren instanceof List);

        Assert.assertEquals(2, resultRootChildren.size());

        boolean hasNodeChild = false;
        boolean hasLeafChild = false;
        for (SpaceDocument child : resultRootChildren) {

            SpaceDocument resultFather = (SpaceDocument) child.getProperty("father");
            Assert.assertNotNull(resultFather);
            Assert.assertTrue(resultFather instanceof SpaceDocument);
            Assert.assertEquals("root", resultFather.getProperty("id"));

            if (child.getTypeName().equals(Node.class.getName())) {
                List<SpaceDocument> resultChildChildren = (List<SpaceDocument>) child.getProperty("children");
                Assert.assertNotNull(resultChildChildren);

                Assert.assertEquals(1, resultChildChildren.size());
                Assert.assertEquals("leaf2", resultChildChildren.get(0).getProperty("id"));
                Assert.assertEquals("leaf1", ((SpaceDocument) resultChildChildren.get(0).getProperty("leftSibling")).getProperty("id"));
                hasNodeChild = true;
            } else {
                Assert.assertEquals(Leaf.class.getName(), child.getTypeName());
                Assert.assertEquals("leaf1", child.getProperty("id"));
                SpaceDocument rightSibling = (SpaceDocument) child.getProperty("rightSibling");
                Assert.assertNotNull(rightSibling);
                Assert.assertTrue(rightSibling instanceof SpaceDocument);
                Assert.assertEquals("leaf2", rightSibling.getProperty("id"));
                hasLeafChild = true;
            }
        }
        Assert.assertTrue(hasLeafChild && hasNodeChild);
    }

//    public boolean cyclicReferenceSupportedEqual(Object obj, VerifiedCyclicReferenceContext context)
//    {
//        if(!(obj instanceof PojoNestedCyclic))
//            return false;
//        PojoNestedCyclic pojo = (PojoNestedCyclic)obj;
//        if(!pojo.getTime().equals(this.time))
//            return false;
//        if(pojo.getTotalLeaves() != (this.totalLeaves))
//            return false;
//        return pojo.getRoot().cyclicReferenceSupportedEqual(root, context);
//    }

    public static class Node {
        String id;
        Node father;
        List<Node> children;

        public Node() {
        }

        public Node getFather() {
            return father;
        }

        public Node setFather(Node father) {
            this.father = father;
            return this;
        }

        public List<Node> getChildren() {
            return children;
        }

        public Node setChildren(List<Node> children) {
            this.children = children;
            return this;
        }

        public String getId() {
            return id;
        }

        public Node setId(String id) {
            this.id = id;
            return this;
        }
//        public boolean cyclicReferenceSupportedEqual(Object obj, VerifiedCyclicReferenceContext context)
//        {
//            if(!(obj instanceof Node))
//                return false;
//            Node node = (Node)obj;
//            if(!node.getId().equals(this.id))
//                return false;
//            if(ObjectUtils.cyclicReferenceSupportedEqual(node.getFather(), father, context) == false)
//                return false;
//            return ObjectUtils.cyclicReferenceSupportedEqual(node.getChildren(), children, context);
//        }
    }

    public static class Leaf extends Node {
        Leaf rightSibling;
        Leaf leftSibling;
        String data;

        public Leaf() {
            super();
        }

        public Leaf getRightSibling() {
            return rightSibling;
        }

        public Leaf setRightSibling(Leaf rightSibling) {
            this.rightSibling = rightSibling;
            return this;
        }

        public Leaf getLeftSibling() {
            return leftSibling;
        }

        public Leaf setLeftSibling(Leaf leftSibling) {
            this.leftSibling = leftSibling;
            return this;
        }

        public String getData() {
            return data;
        }

        public Leaf setData(String data) {
            this.data = data;
            return this;
        }

    }

}
