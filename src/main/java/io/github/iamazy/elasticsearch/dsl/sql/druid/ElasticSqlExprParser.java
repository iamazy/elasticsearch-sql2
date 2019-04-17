package io.github.iamazy.elasticsearch.dsl.sql.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.parser.*;
import com.google.common.collect.Lists;
import java.util.List;

/**
 * @author iamazy
 * @date 2019/2/19
 **/
public class ElasticSqlExprParser extends SQLExprParser {

    public ElasticSqlExprParser(Lexer lexer) {
        super(lexer);
    }

    public ElasticSqlExprParser(String sql) {
        this(new ElasticSqlLexer(sql));
        this.lexer.nextToken();
    }

    @Override
    public SQLExpr expr() {
        if (this.lexer.token() == Token.STAR) {
            this.lexer.nextToken();
            SQLExpr expr = new SQLAllColumnExpr();
            if (this.lexer.token() == Token.DOT) {
                this.lexer.nextToken();
                this.accept(Token.STAR);
                return new SQLPropertyExpr(expr, "*");
            } else {
                return expr;
            }
        } else {
            SQLExpr expr = this.primary();
            Token token = this.lexer.token();
            if (token == Token.COMMA) {
                return expr;
            } else if (token == Token.EQ) {
                expr = this.relationalRest(expr);
                expr = this.andRest(expr);
                expr = this.xorRest(expr);
                expr = this.orRest(expr);
                return expr;
            } else {
                return this.exprRest(expr);
            }
        }
    }

    @Override
    public SQLSelectParser createSelectParser() {
        return new ElasticSqlSelectParser(this);
    }

    ElasticSqlSelectQueryBlock.Scroll parseScroll() {
        if (lexer.token() == Token.CURSOR && "scroll".equalsIgnoreCase(lexer.stringVal())) {
            lexer.nextToken();
            ElasticSqlSelectQueryBlock.Scroll scroll = new ElasticSqlSelectQueryBlock.Scroll();
            accept(Token.BY);
            scroll.setExpire(this.expr());
            if (this.lexer.token() == Token.COMMA) {
                this.lexer.nextToken();
                scroll.setScrollId(this.expr());
            }
            return scroll;
        }
        return null;
    }

    ElasticSqlSelectQueryBlock.Limit parseLimit0() {
        if (lexer.token() == Token.LIMIT) {
            lexer.nextToken();
            ElasticSqlSelectQueryBlock.Limit limit = new ElasticSqlSelectQueryBlock.Limit();
            SQLExpr temp = this.expr();
            if (lexer.token() == Token.COMMA) {
                limit.setOffset(temp);
                lexer.nextToken();
                limit.setRowCount(this.expr());
            } else if (identifierEquals("OFFSET")) {
                limit.setRowCount(temp);
                lexer.nextToken();
                limit.setOffset(this.expr());
            } else {
                limit.setRowCount(temp);
            }
            return limit;
        }
        return null;
    }

    ElasticSqlSelectQueryBlock.Routing parseRoutingBy() {
        if (lexer.token() == Token.INDEX && "routing".equalsIgnoreCase(lexer.stringVal())) {
            lexer.nextToken();
            accept(Token.BY);
            List<SQLExpr> routingValues = Lists.newLinkedList();
            routingValues.add(this.expr());
            while (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                routingValues.add(this.expr());
            }
            return new ElasticSqlSelectQueryBlock.Routing(routingValues);
        }
        return null;
    }
}
