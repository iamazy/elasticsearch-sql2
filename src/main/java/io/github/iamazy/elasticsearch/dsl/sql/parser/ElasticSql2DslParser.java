package io.github.iamazy.elasticsearch.dsl.sql.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.parser.Token;
import com.google.common.collect.ImmutableList;
import io.github.iamazy.elasticsearch.dsl.sql.druid.ElasticSqlExprParser;
import io.github.iamazy.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.parser.aggs.GroupByAggregationParser;
import io.github.iamazy.elasticsearch.dsl.sql.parser.sql.*;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticDslContext;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;


import java.util.List;

/**
 * @author iamazy
 */
public class ElasticSql2DslParser {

    public ElasticSqlParseResult parse(String sql) throws ElasticSql2DslException {
        SQLQueryExpr queryExpr;
        try {
            SQLStatementParser sqlStatementParser = new SQLStatementParser(sql);
            Token token = sqlStatementParser.getLexer().token();
            switch (token) {
                case DELETE: {
                    SQLDeleteStatement sqlDeleteStatement = sqlStatementParser.parseDeleteStatement();
                    SQLLimit sqlLimit = sqlStatementParser.getExprParser().parseLimit();
                    ElasticDslContext elasticDslContext = new ElasticDslContext(sqlDeleteStatement);
                    for (QueryParser sqlParser : buildSqlDeleteParserChain()) {
                        sqlParser.parse(elasticDslContext);
                    }
                    //此处设置的是DeleteByQueryRequest的Size，将DeleteByQueryRequest中的SearchRequest的DSL打印出来的size是1000，不是这个值，不要搞混淆
                    elasticDslContext.getParseResult().setSize(((SQLIntegerExpr) sqlLimit.getRowCount()).getNumber().intValue());
                    return elasticDslContext.getParseResult();
                }
                case SELECT:
                default: {
                    ElasticSqlExprParser elasticSqlExprParser = new ElasticSqlExprParser(sql);
                    SQLExpr sqlQueryExpr = elasticSqlExprParser.expr();
                    check(elasticSqlExprParser, sqlQueryExpr);
                    queryExpr = (SQLQueryExpr) sqlQueryExpr;
                    ElasticDslContext elasticDslContext = new ElasticDslContext(queryExpr);
                    if (queryExpr.getSubQuery().getQuery() instanceof ElasticSqlSelectQueryBlock) {
                        for (QueryParser sqlParser : buildSqlSelectParserChain()) {
                            sqlParser.parse(elasticDslContext);
                        }
                    } else {
                        throw new ElasticSql2DslException("[syntax error] Sql only support Select,Delete Sql");
                    }
                    return elasticDslContext.getParseResult();
                }
            }

        } catch (ParserException ex) {
            throw new ElasticSql2DslException(ex);
        }

    }

    private void check(ElasticSqlExprParser sqlExprParser, SQLExpr sqlQueryExpr) {
        if (sqlExprParser.getLexer().token() != Token.EOF) {
            throw new ElasticSql2DslException("[syntax error] Sql last token is not EOF");
        }

        if (!(sqlQueryExpr instanceof SQLQueryExpr)) {
            throw new ElasticSql2DslException("[syntax error] Sql is not select druid");
        }
    }

    private List<QueryParser> buildSqlSelectParserChain( ) {
        //SQL解析器的顺序不能改变
        return ImmutableList.of(
                //解析SQL指定的索引和文档类型
                new QueryFromParser(),
                //解析SQL查询指定的match条件
                new QueryMatchConditionParser(),
                //解析SQL查询指定的where条件
                new QueryWhereConditionParser(),
                //解析SQL排序条件
                new QueryOrderConditionParser(),
                //解析路由参数
                new QueryRoutingValParser(),
                //解析分组统计
                new GroupByAggregationParser(),
                //解析SQL查询指定的字段
                new QuerySelectFieldListParser(),
                //解析Scroll By字段
                new QueryScrollParser(),
                //解析SQL的分页条数
                new QueryLimitSizeParser()
        );
    }

    private List<QueryParser> buildSqlDeleteParserChain() {
        //SQL解析器的顺序不能改变
        return ImmutableList.of(
                //解析SQL指定的索引和文档类型
                new QueryFromParser(),
                //解析SQL查询指定的where条件
                new QueryWhereConditionParser(),
                //解析路由参数
                new QueryRoutingValParser()
        );
    }
}
