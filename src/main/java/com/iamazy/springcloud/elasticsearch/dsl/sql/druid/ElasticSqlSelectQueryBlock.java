package com.iamazy.springcloud.elasticsearch.dsl.sql.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLObjectImpl;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
import org.apache.commons.collections4.CollectionUtils;

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

public class ElasticSqlSelectQueryBlock extends SQLSelectQueryBlock implements SQLObject {
    private Limit limit;
    private Routing routing;
    private SQLExpr matchQuery;

    public Limit getLimit0() {
        return limit;
    }

    public SQLExpr getMatchQuery() {
        return matchQuery;
    }

    public void setMatchQuery(SQLExpr matchQuery) {
        this.matchQuery = matchQuery;
    }

    public void setLimit(Limit limit) {
        this.limit = limit;
    }

    public Routing getRouting() {
        return routing;
    }

    public void setRouting(Routing routing) {
        this.routing = routing;
    }

    public static class Routing extends SQLObjectImpl{
        private List<SQLExpr> routingValues;
        public Routing(List<SQLExpr> routingValues){
            this.routingValues=routingValues;
            if(CollectionUtils.isNotEmpty(routingValues)){
                for(SQLExpr sqlExpr:routingValues){
                    sqlExpr.setParent(Routing.this);
                }
            }
        }

        @Override
        protected void accept0(SQLASTVisitor sqlastVisitor) {
            throw new UnsupportedOperationException("accept0(SQLASTVisitor visitor)");
        }

        public List<SQLExpr> getRoutingValues(){
            return routingValues;
        }
    }

    public static class Limit extends SQLObjectImpl{
        private SQLExpr rowCount;
        private SQLExpr offset;

        public SQLExpr getRowCount() {
            return rowCount;
        }

        public void setRowCount(SQLExpr rowCount) {
            if(rowCount!=null){
                rowCount.setParent(this);
            }
            this.rowCount = rowCount;
        }

        public SQLExpr getOffset(){
            return offset;
        }

        public void setOffset(SQLExpr offset) {
            if(offset!=null){
                offset.setParent(this);
            }
            this.offset = offset;
        }

        @Override
        protected void accept0(SQLASTVisitor sqlastVisitor) {
            throw new UnsupportedOperationException("accept0(SQLASTVisitor visitor)");
        }
    }
}
