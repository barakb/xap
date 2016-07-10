package com.gigaspaces.metadata.pojos;

/**
 * @author: yaeln
 * @since: 12.0.0
 */

import com.gigaspaces.annotation.pojo.SpaceClassConstructor;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceProperty;

public class PojoWithSpaceClassConstructorAndSetter {

    private String id;
    private String name;

    @SpaceClassConstructor
    public PojoWithSpaceClassConstructorAndSetter(String id) {
        this.id = id;
    }

    @SpaceId
    public String getId() {
        return id;
    }


    @SpaceProperty
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "PojoWithSpaceClassConstructorAndSetter{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
