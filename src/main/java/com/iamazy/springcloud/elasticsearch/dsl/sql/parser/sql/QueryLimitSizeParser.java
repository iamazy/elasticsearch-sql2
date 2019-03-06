package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.iamazy.springcloud.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import com.iamazy.springcloud.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticDslContext;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.SqlArgs;


public class QueryLimitSizeParser implements QueryParser {

    private ParseActionListener parseActionListener;

    public QueryLimitSizeParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    @Override
    public void parse(ElasticDslContext dslContext) {

        if (dslContext.getSqlObject() instanceof SQLQueryExpr) {
            ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) ((SQLQueryExpr) dslContext.getSqlObject()).getSubQuery().getQuery();
            if (queryBlock.getLimit0() != null) {
                Integer from = parseLimitInteger(queryBlock.getLimit0().getOffset(), dslContext.getSqlArgs());
                dslContext.getParseResult().setFrom(from);

                Integer size = parseLimitInteger(queryBlock.getLimit0().getRowCount(), dslContext.getSqlArgs());
                dslContext.getParseResult().setSize(size);

                parseActionListener.onLimitSizeParse(from, size);
            } else {
                dslContext.getParseResult().setFrom(0);
                dslContext.getParseResult().setSize(15);
            }
        }
    }

    private Integer parseLimitInteger(SQLExpr limitInt, SqlArgs args) {
        if (limitInt instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) limitInt).getNumber().intValue();
        } else if (limitInt instanceof SQLVariantRefExpr) {
            SQLVariantRefExpr varLimitExpr = (SQLVariantRefExpr) limitInt;
            Object targetVal = ElasticSqlArgConverter.convertSqlArg(varLimitExpr, args);
            if (!(targetVal instanceof Integer)) {
                throw new ElasticSql2DslException("[syntax error] Sql limit expr should be a non-negative number");
            }
            return Integer.valueOf(targetVal.toString());
        } else {
            throw new ElasticSql2DslException("[syntax error] Sql limit expr should be a non-negative number");
        }
    }
}
