package io.github.iamazy.elasticsearch.dsl.sql.parser.sql;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.google.common.collect.Lists;
import io.github.iamazy.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticDslContext;


public class QueryFromParser implements QueryParser {

    @Override
    public void parse(ElasticDslContext dslContext) {
        SQLExprTableSource tableSource;
        if (dslContext.getSqlObject() instanceof SQLDeleteStatement) {
            SQLDeleteStatement sqlDeleteStatement = (SQLDeleteStatement) dslContext.getSqlObject();
            tableSource = sqlDeleteStatement.getExprTableSource();
            extractFrom(tableSource,dslContext);
        }

        if (dslContext.getSqlObject() instanceof SQLQueryExpr) {
            ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) ((SQLQueryExpr) dslContext.getSqlObject()).getSubQuery().getQuery();
            if (queryBlock.getFrom() instanceof SQLExprTableSource) {
                tableSource = (SQLExprTableSource) queryBlock.getFrom();
                extractFrom(tableSource,dslContext);
            }

        }
    }


    private void extractFrom(SQLExprTableSource tableSource,ElasticDslContext dslContext){
        dslContext.getParseResult().setQueryAs(tableSource.getAlias());

        if (tableSource.getExpr() instanceof SQLIdentifierExpr) {
            String index = ((SQLIdentifierExpr) tableSource.getExpr()).getName();
            dslContext.getParseResult().setIndices(Lists.newArrayList(index));
            return;
        }

        if (tableSource.getExpr() instanceof SQLPropertyExpr) {
            SQLPropertyExpr idxExpr = (SQLPropertyExpr) tableSource.getExpr();

            if (!(idxExpr.getOwner() instanceof SQLIdentifierExpr)) {
                throw new ElasticSql2DslException("[syntax error] From table should like [index].[type]");
            }

            String index = ((SQLIdentifierExpr) idxExpr.getOwner()).getName();
            dslContext.getParseResult().setIndices(Lists.newArrayList(index));
//            dslContext.getParseResult().setType(idxExpr.getName());
            return;
        }

        throw new ElasticSql2DslException("[syntax error] From table should like [index].[type]");
    }
}
