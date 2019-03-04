package com.iamazy.springcloud.elasticsearch.dsl.sql.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.SQLSelectParser;
import com.alibaba.druid.sql.parser.Token;

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
public class ElasticSqlSelectParser extends SQLSelectParser {

    public ElasticSqlSelectParser(SQLExprParser exprParser){
        super(exprParser);
    }

    public ElasticSqlSelectQueryBlock.Limit parseLimit(){
        return ((ElasticSqlExprParser)this.exprParser).parseLimit0();
    }

    public ElasticSqlSelectQueryBlock.Routing parseRoutingBy(){
        return ((ElasticSqlExprParser)this.exprParser).parseRourtingBy();
    }

    @Override
    public SQLSelectQuery query() {
        if(lexer.token()== Token.LPAREN){
            lexer.nextToken();
            SQLSelectQuery select=query();
            accept(Token.RPAREN);
            return queryRest(select);
        }
        accept(Token.SELECT);
        if(lexer.token()==Token.COMMENT){
            lexer.nextToken();
        }
        ElasticSqlSelectQueryBlock queryBlock=new ElasticSqlSelectQueryBlock();
        if(lexer.token()==Token.DISTINCT){
            queryBlock.setDistionOption(SQLSetQuantifier.DISTINCT);
            lexer.nextToken();
        }
        else if(lexer.token()==Token.UNIQUE){
            queryBlock.setDistionOption(SQLSetQuantifier.UNIQUE);
            lexer.nextToken();
        }
        else if(lexer.token()==Token.ALL){
            queryBlock.setDistionOption(SQLSetQuantifier.ALL);
            lexer.nextToken();
        }
        parseSelectList(queryBlock);
        parseFrom(queryBlock);
        parseMatchQuery(queryBlock);
        parseWhere(queryBlock);
        parseGroupBy(queryBlock);
        queryBlock.setOrderBy(this.exprParser.parseOrderBy());

        if(lexer.token()==Token.INDEX&&"ROUTING".equalsIgnoreCase(lexer.stringVal())){
            queryBlock.setRouting(parseRoutingBy());
        }
        if(lexer.token()==Token.LIMIT){
            queryBlock.setLimit(parseLimit());
        }
        return queryRest(queryBlock);
    }

    @Override
    public SQLTableSource parseTableSource() {
        if(lexer.token()!=Token.IDENTIFIER){
            throw new ParserException("[syntax error] from table source is not a identifier");
        }
        SQLExprTableSource tableSource=new SQLExprTableSource();
        parseTableSourceQueryTableExpr(tableSource);
        SQLTableSource tableSrc=parseTableSourceRest(tableSource);
        if(lexer.hasComment()&&lexer.isKeepComments()){
            tableSrc.addAfterComment(lexer.readAndResetComments());
        }
        return tableSrc;
    }

    private void parseMatchQuery(ElasticSqlSelectQueryBlock queryBlock){
        if(lexer.token()==Token.INDEX&&"QUERY".equalsIgnoreCase(lexer.stringVal())){
            lexer.nextToken();
            SQLExpr matchQuery=expr();
            queryBlock.setMatchQuery(matchQuery);
        }
    }
}
