package com.iamazy.elasticsearch.dsl.sql.model;

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
    private ElasticSqlParseResult parseResult;

    public ElasticDslContext(SQLObject sqlObject){

        this.sqlObject =sqlObject;
        parseResult=new ElasticSqlParseResult();
    }


    @Override
    public String toString() {
        return parseResult.toDsl(parseResult.toRequest());
    }
}
