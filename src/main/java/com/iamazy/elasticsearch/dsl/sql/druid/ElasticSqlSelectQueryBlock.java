package com.iamazy.elasticsearch.dsl.sql.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLObjectImpl;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/

public class ElasticSqlSelectQueryBlock extends SQLSelectQueryBlock implements SQLObject {

    private Scroll scroll;
    private Limit limit;
    private Routing routing;
    private SQLExpr matchQuery;

    public Scroll getScroll() {
        return scroll;
    }

    public void setScroll(Scroll scroll) {
        this.scroll = scroll;
    }

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


    public static class Scroll extends SQLObjectImpl{

        private SQLExpr expire;
        private SQLExpr scrollId;

        public SQLExpr getExpire() {
            return expire;
        }

        public SQLExpr getScrollId() {
            return scrollId;
        }

        public void setExpire(SQLExpr expire) {
            if(expire!=null){
                expire.setParent(this);
            }
            this.expire = expire;
        }

        public void setScrollId(SQLExpr scrollId) {
            if(scrollId!=null){
                scrollId.setParent(this);
            }
            this.scrollId = scrollId;
        }

        @Override
        protected void accept0(SQLASTVisitor sqlastVisitor) {
            throw new UnsupportedOperationException("accept0(SQLASTVisitor visitor)");
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
