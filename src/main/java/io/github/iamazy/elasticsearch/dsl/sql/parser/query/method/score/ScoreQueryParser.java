package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.score;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodQueryParser;
import lombok.Data;

import java.util.List;


/**
 * @author iamazy
 * @date 2019/4/9
 * @descrition
 **/
@Data
public class ScoreQueryParser {

    private final List<MethodQueryParser> scoreQueryParsers;

    public ScoreQueryParser() {
        scoreQueryParsers = ImmutableList.of(
                new BoostingQueryParser(),
                new FunctionScoreQueryParser()
        );
    }

    public Boolean isScoreAtomQuery(MethodInvocation invocation) {
        return scoreQueryParsers.stream().anyMatch(methodQueryParser -> methodQueryParser.isMatchMethodInvocation(invocation));
    }

    public AtomicQuery parseScoreAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs) {
        MethodInvocation methodInvocation = new MethodInvocation(methodQueryExpr, queryAs);
        MethodQueryParser joinAtomQueryParser = getQueryParser(methodInvocation);
        joinAtomQueryParser.checkMethodInvocation(methodInvocation);
        return joinAtomQueryParser.parseMethodQuery(methodInvocation);
    }

    private MethodQueryParser getQueryParser(MethodInvocation methodInvocation) {
        for (MethodQueryParser scoreQueryParserItem : scoreQueryParsers) {
            if (scoreQueryParserItem.isMatchMethodInvocation(methodInvocation)) {
                return scoreQueryParserItem;
            }
        }
        throw new ElasticSql2DslException(
                String.format("[syntax error] Can not support score query expr[%s] condition",
                        methodInvocation.getMethodName()));
    }
}
