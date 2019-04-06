package io.github.iamazy.elasticsearch.dsl.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import io.github.iamazy.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticDslContext;


public class QueryLimitSizeParser implements QueryParser {



    public QueryLimitSizeParser() { }

    @Override
    public void parse(ElasticDslContext dslContext) {

        if (dslContext.getSqlObject() instanceof SQLQueryExpr) {
            ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) ((SQLQueryExpr) dslContext.getSqlObject()).getSubQuery().getQuery();
            if (queryBlock.getLimit0() != null) {
                Integer from = parseLimitInteger(queryBlock.getLimit0().getOffset());
                dslContext.getParseResult().setFrom(from);

                Integer size = parseLimitInteger(queryBlock.getLimit0().getRowCount());
                dslContext.getParseResult().setSize(size);
            } else {
                dslContext.getParseResult().setFrom(0);
                dslContext.getParseResult().setSize(15);
            }
        }
    }

    private Integer parseLimitInteger(SQLExpr limitInt) {
        if (limitInt instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) limitInt).getNumber().intValue();
        } else if (limitInt instanceof SQLVariantRefExpr) {
            SQLVariantRefExpr varLimitExpr = (SQLVariantRefExpr) limitInt;
            Object targetVal = ElasticSqlArgConverter.convertSqlArg(varLimitExpr);
            if (!(targetVal instanceof Integer)) {
                throw new ElasticSql2DslException("[syntax error] Sql limit expr should be a non-negative number");
            }
            return Integer.valueOf(targetVal.toString());
        } else {
            throw new ElasticSql2DslException("[syntax error] Sql limit expr should be a non-negative number");
        }
    }
}
