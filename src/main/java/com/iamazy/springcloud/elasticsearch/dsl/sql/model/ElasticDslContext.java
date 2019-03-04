package com.iamazy.springcloud.elasticsearch.dsl.sql.model;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import lombok.Getter;

/**
 * Copyright 2018-2019 iamazy Logic Ltd
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
