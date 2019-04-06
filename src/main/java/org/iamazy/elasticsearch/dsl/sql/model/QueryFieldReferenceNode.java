package org.iamazy.elasticsearch.dsl.sql.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/

@Getter
@AllArgsConstructor
public class QueryFieldReferenceNode {
    @Setter
    private boolean isNestedDocReference;
    private String referenceNodeName;

    public QueryFieldReferenceNode(String referenceNodeName, boolean isNestedDocReference) {
        this.isNestedDocReference = isNestedDocReference;
        this.referenceNodeName = referenceNodeName;
    }
}
