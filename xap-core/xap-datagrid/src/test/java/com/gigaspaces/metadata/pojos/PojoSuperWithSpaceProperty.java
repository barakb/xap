package com.gigaspaces.metadata.pojos;

/**
 * @author: yaeln
 * @since: 12.0.0
 */

import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpaceProperty;

public abstract class PojoSuperWithSpaceProperty {

    private Integer metaId;

    @SpaceProperty
    @SpaceIndex
    public Integer getMetaId() {
        return metaId;
    }

    public void setMetaId(Integer metaId) {
        this.metaId = metaId;
    }
}

