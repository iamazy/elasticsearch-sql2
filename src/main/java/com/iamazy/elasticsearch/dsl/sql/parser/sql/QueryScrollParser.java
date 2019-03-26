package com.iamazy.elasticsearch.dsl.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.iamazy.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.elasticsearch.dsl.sql.model.ElasticDslContext;


/**
 * @author iamazy
 * @date 2019/3/25
 * @descrition
 **/
public class QueryScrollParser implements QueryParser  {

    private ParseActionListener parseActionListener;

    public QueryScrollParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

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
                    parseActionListener.onScrollParse(expire, scrollId);
                }
                parseActionListener.onScrollParse(expire,null);
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
