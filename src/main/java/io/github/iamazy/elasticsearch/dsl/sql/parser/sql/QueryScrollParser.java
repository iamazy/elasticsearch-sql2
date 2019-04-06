package io.github.iamazy.elasticsearch.dsl.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import io.github.iamazy.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticDslContext;


/**
 * @author iamazy
 * @date 2019/3/25
 * @descrition
 **/
public class QueryScrollParser implements QueryParser  {

    public QueryScrollParser() { }

    @Override
    public void parse(ElasticDslContext dslContext) {

        if (dslContext.getSqlObject() instanceof SQLQueryExpr) {
            ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) ((SQLQueryExpr) dslContext.getSqlObject()).getSubQuery().getQuery();
            if (queryBlock.getScroll() != null) {
                String expire = parseScroll(queryBlock.getScroll().getExpire());
                dslContext.getParseResult().setScrollExpire(expire);
                if(queryBlock.getScroll().getScrollId()!=null) {
                    String scrollId = parseScroll(queryBlock.getScroll().getScrollId());
                    dslContext.getParseResult().setScrollId(scrollId);
                }
            }
        }
    }

    private String parseScroll(SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLCharExpr) {
            return ((SQLCharExpr) sqlExpr).getText();
        } else {
            throw new ElasticSql2DslException("[syntax error] Sql scroll expr should be a string expr");
        }
    }
}
