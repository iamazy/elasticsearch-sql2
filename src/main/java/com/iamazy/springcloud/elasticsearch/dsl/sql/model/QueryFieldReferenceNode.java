package com.iamazy.springcloud.elasticsearch.dsl.sql.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/

@Getter
@AllArgsConstructor
public class QueryFieldReferenceNode {
    private boolean isNestedDocReference;
    private String referenceNodeName;

    public QueryFieldReferenceNode(String referenceNodeName, boolean isNestedDocReference) {
        this.isNestedDocReference = isNestedDocReference;
        this.referenceNodeName = referenceNodeName;
    }
}
