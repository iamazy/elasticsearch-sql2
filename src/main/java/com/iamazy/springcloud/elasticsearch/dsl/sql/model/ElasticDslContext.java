package com.iamazy.springcloud.elasticsearch.dsl.sql.model;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import lombok.Getter;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/

@Getter
public class ElasticDslContext {

    private SQLObject sqlObject;
    private SqlArgs sqlArgs;
    private ElasticSqlParseResult parseResult;

    public ElasticDslContext(SQLObject sqlObject, SqlArgs sqlArgs){
        this.sqlArgs=sqlArgs;
        this.sqlObject =sqlObject;
        parseResult=new ElasticSqlParseResult();
    }

    public ElasticDslContext(SQLQueryExpr sqlQueryExpr){
        this.sqlObject =sqlQueryExpr;
        parseResult=new ElasticSqlParseResult();
    }

    @Override
    public String toString() {
        return parseResult.toDsl(parseResult.toRequest());
    }
}
