package io.github.iamazy.elasticsearch.dsl.sql.model;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collections;
import java.util.List;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/
public class QueryFieldReferencePath {
    private List<QueryFieldReferenceNode> referenceNodes;
    public void addReferenceNode(QueryFieldReferenceNode referenceNode){
        if(referenceNodes==null){
            referenceNodes= Lists.newLinkedList();
        }
        referenceNodes.add(referenceNode);
    }

    public List<QueryFieldReferenceNode> getReferenceNodes(){
        if(CollectionUtils.isEmpty(referenceNodes)){
            return Collections.emptyList();
        }
        return ImmutableList.copyOf(referenceNodes);
    }
}
