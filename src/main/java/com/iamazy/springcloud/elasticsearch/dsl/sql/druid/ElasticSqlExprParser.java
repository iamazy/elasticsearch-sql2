package com.iamazy.springcloud.elasticsearch.dsl.sql.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.SQLSelectParser;
import com.alibaba.druid.sql.parser.Token;
import com.google.common.collect.Lists;

import java.util.List;

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
public class ElasticSqlExprParser extends SQLExprParser {

    public ElasticSqlExprParser(Lexer lexer){
        super(lexer);
    }

    public ElasticSqlExprParser(String sql){
        this(new ElasticSqlLexer(sql));
        this.lexer.nextToken();
    }

    @Override
    public SQLSelectParser createSelectParser() {
        return new ElasticSqlSelectParser(this);
    }

    public SQLDeleteStatement createDeleteStatement(String sql){
        return new SQLDeleteStatement(sql);
    }

    public ElasticSqlSelectQueryBlock.Limit parseLimit0(){
        if(lexer.token()== Token.LIMIT){
            lexer.nextToken();
            ElasticSqlSelectQueryBlock.Limit limit=new ElasticSqlSelectQueryBlock.Limit();
            SQLExpr temp=this.expr();
            if(lexer.token()==Token.COMMA){
                limit.setOffset(temp);
                lexer.nextToken();
                limit.setRowCount(this.expr());
            }else if(identifierEquals("OFFSET")){
                limit.setRowCount(temp);
                lexer.nextToken();
                limit.setOffset(this.expr());
            }else{
                limit.setRowCount(temp);
            }
            return limit;
        }
        return null;
    }

    public ElasticSqlSelectQueryBlock.Routing parseRourtingBy(){
        if(lexer.token()==Token.INDEX&&"routing".equalsIgnoreCase(lexer.stringVal())){
            lexer.nextToken();
            accept(Token.BY);
            List<SQLExpr> routingValues= Lists.newLinkedList();
            routingValues.add(this.expr());
            while (lexer.token()==Token.COMMA){
                lexer.nextToken();
                routingValues.add(this.expr());
            }
            return new ElasticSqlSelectQueryBlock.Routing(routingValues);
        }
        return null;
    }
}
