package com.gigaspaces.metadata.pojos;

import com.gigaspaces.annotation.pojo.SpaceClassConstructor;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceProperty;

/**
 * @author: yaeln
 * @since: 6/30/16.
 */
public class PojoWithFieldInConstructorAndHasSetter {

    private String id;
    private String name;

    @SpaceClassConstructor
    public PojoWithFieldInConstructorAndHasSetter(String id, String name) {
        this.id = id;
        this.name = name;
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
}
