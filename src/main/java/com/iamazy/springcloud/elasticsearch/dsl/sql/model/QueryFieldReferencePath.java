package com.iamazy.springcloud.elasticsearch.dsl.sql.model;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collections;
import java.util.List;

/**
 * Copyright 2018-2019 iamazy Logic Ltd
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
