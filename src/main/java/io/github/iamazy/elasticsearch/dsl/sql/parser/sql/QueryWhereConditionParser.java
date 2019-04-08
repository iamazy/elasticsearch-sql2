package io.github.iamazy.elasticsearch.dsl.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import io.github.iamazy.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticDslContext;
import org.elasticsearch.index.query.BoolQueryBuilder;


public class QueryWhereConditionParser extends BoolExpressionParser implements QueryParser{


    @Override
    public void parse(ElasticDslContext dslContext) {

        if(dslContext.getSqlObject() instanceof SQLDeleteStatement){
            SQLDeleteStatement sqlDeleteStatement = (SQLDeleteStatement) dslContext.getSqlObject();
            String queryAs=dslContext.getParseResult().getQueryAs();
            SQLExpr sqlExpr=sqlDeleteStatement.getWhere();
            BoolQueryBuilder matchQuery=parseBoolQueryExpr(sqlExpr,queryAs);
            dslContext.getParseResult().setMatchCondition(matchQuery);
        }
        if(dslContext.getSqlObject() instanceof SQLQueryExpr) {
            ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) ((SQLQueryExpr) dslContext.getSqlObject()).getSubQuery().getQuery();

            if (queryBlock.getWhere() != null) {
                String queryAs = dslContext.getParseResult().getQueryAs();
                BoolQueryBuilder whereQuery = parseBoolQueryExpr(queryBlock.getWhere(), queryAs);
                dslContext.getParseResult().setWhereCondition(whereQuery);
                dslContext.getParseResult().getHighlighter().addAll(this.getHighlighter());
            }
        }
    }
}
