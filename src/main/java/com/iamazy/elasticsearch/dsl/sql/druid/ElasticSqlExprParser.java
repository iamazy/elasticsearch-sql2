package com.iamazy.elasticsearch.dsl.sql.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.SQLSelectParser;
import com.alibaba.druid.sql.parser.Token;
import com.google.common.collect.Lists;

import java.util.List;

/**
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
    public SQLExpr expr() {
        return super.expr();
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
