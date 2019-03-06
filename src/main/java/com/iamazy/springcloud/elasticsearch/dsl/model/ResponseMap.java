package com.iamazy.springcloud.elasticsearch.dsl.model;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author iamazy
 * @date 2019/2/23
 * @descrition
 **/

@Data
@NoArgsConstructor
public class ResponseMap {

    private Integer status;

    private Object body;

    private String info;
}
