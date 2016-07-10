package com.gigaspaces.metadata.pojos;

import com.gigaspaces.annotation.pojo.SpaceClassConstructor;
import com.gigaspaces.annotation.pojo.SpaceId;

/**
 * @author: yaeln
 * @since: 12.0.0
 */

public class PojoExtendsWithSpaceClassConstructor extends PojoSuperWithSpaceProperty {

    private String id;
    private String name;

    @SpaceClassConstructor
    public PojoExtendsWithSpaceClassConstructor(String id, String name) {
        this.id = id;
        this.name = name;
    }

    @SpaceId
    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
