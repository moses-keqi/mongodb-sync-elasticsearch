package com.moses.sync.enums;

import lombok.Getter;

/**
 * @author HanKeQi
 * @Description
 * @date 2019/4/20 12:24 PM
 **/

public enum MongoDdl {

    i("insert"),
    u("update"),
    d("delete");

    @Getter
    private String name;

    MongoDdl(String name){
        this.name = name;
    }
}
