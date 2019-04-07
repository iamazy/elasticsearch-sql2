package io.github.iamazy.elasticsearch.dsl.sql.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.db2.ast.stmt.DB2SelectQueryBlock;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.SQLSelectParser;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.druid.util.FnvHash;
import com.alibaba.druid.util.JdbcConstants;

/**
 * @author iamazy
 * @date 2019/2/19
 **/
public class ElasticSqlSelectParser extends SQLSelectParser {

    ElasticSqlSelectParser(SQLExprParser exprParser){
        super(exprParser);
    }

    private ElasticSqlSelectQueryBlock.Limit parseLimit(){
        return ((ElasticSqlExprParser)this.exprParser).parseLimit0();
    }

    private ElasticSqlSelectQueryBlock.Routing parseRoutingBy(){
        return ((ElasticSqlExprParser)this.exprParser).parseRourtingBy();
    }

    private ElasticSqlSelectQueryBlock.Scroll parseScroll(){
        return ((ElasticSqlExprParser)this.exprParser).parseScroll();
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

        //代码顺序不能改变
        parseSelectList(queryBlock);
        parseFrom(queryBlock);
        parseMatchQuery(queryBlock);
        parseWhere(queryBlock);
        parseGroupBy(queryBlock);
        queryBlock.setOrderBy(this.exprParser.parseOrderBy());


        if(lexer.token()==Token.INDEX&&"ROUTING".equalsIgnoreCase(lexer.stringVal())){
            queryBlock.setRouting(parseRoutingBy());
        }
        if(lexer.token()==Token.CURSOR&&"SCROLL".equalsIgnoreCase(lexer.stringVal())){
            queryBlock.setScroll(parseScroll());
        }
        if(lexer.token()==Token.LIMIT){
            queryBlock.setLimit(parseLimit());
        }

        return queryRest(queryBlock);
    }

    @Override
    protected void parseGroupBy(SQLSelectQueryBlock queryBlock) {
        if (lexer.token() == (Token.GROUP)) {
            lexer.nextTokenBy();
            accept(Token.BY);

            SQLSelectGroupByClause groupBy = new SQLSelectGroupByClause();
            if (lexer.identifierEquals(FnvHash.Constants.ROLLUP)) {
                lexer.nextToken();
                accept(Token.LPAREN);
                groupBy.setWithRollUp(true);
            }
            if (lexer.identifierEquals(FnvHash.Constants.CUBE)) {
                lexer.nextToken();
                accept(Token.LPAREN);
                groupBy.setWithCube(true);
            }

            for (;;) {
                SQLExpr item = parseGroupByItem();

                item.setParent(groupBy);
                groupBy.addItem(item);

                if (lexer.token() != Token.COMMA&&lexer.token()!=Token.GT) {
                    break;
                }

                lexer.nextToken();
            }
            if (groupBy.isWithRollUp() || groupBy.isWithCube()) {
                accept(Token.RPAREN);
            }

            if (lexer.token() == Token.HAVING) {
                lexer.nextToken();

                SQLExpr having = this.exprParser.expr();
                groupBy.setHaving(having);
            }

            if (lexer.token() == Token.WITH) {
                lexer.nextToken();

                if (lexer.identifierEquals(FnvHash.Constants.CUBE)) {
                    lexer.nextToken();
                    groupBy.setWithCube(true);
                } else if(lexer.identifierEquals(FnvHash.Constants.ROLLUP)) {
                    lexer.nextToken();
                    groupBy.setWithRollUp(true);
                } else if (lexer.identifierEquals(FnvHash.Constants.RS)
                        && JdbcConstants.DB2.equals(dbType)) {
                    lexer.nextToken();
                    ((DB2SelectQueryBlock) queryBlock).setIsolation(DB2SelectQueryBlock.Isolation.RS);
                } else if (lexer.identifierEquals(FnvHash.Constants.RR)
                        && JdbcConstants.DB2.equals(dbType)) {
                    lexer.nextToken();
                    ((DB2SelectQueryBlock) queryBlock).setIsolation(DB2SelectQueryBlock.Isolation.RR);
                } else if (lexer.identifierEquals(FnvHash.Constants.CS)
                        && JdbcConstants.DB2.equals(dbType)) {
                    lexer.nextToken();
                    ((DB2SelectQueryBlock) queryBlock).setIsolation(DB2SelectQueryBlock.Isolation.CS);
                } else if (lexer.identifierEquals(FnvHash.Constants.UR)
                        && JdbcConstants.DB2.equals(dbType)) {
                    lexer.nextToken();
                    ((DB2SelectQueryBlock) queryBlock).setIsolation(DB2SelectQueryBlock.Isolation.UR);
                } else {
                    throw new ParserException("TODO " + lexer.info());
                }
            }

            queryBlock.setGroupBy(groupBy);
        } else if (lexer.token() == Token.HAVING) {
            lexer.nextToken();

            SQLSelectGroupByClause groupBy = new SQLSelectGroupByClause();
            groupBy.setHaving(this.exprParser.expr());

            if (lexer.token() == Token.GROUP) {
                lexer.nextToken();
                accept(Token.BY);

                for (;;) {
                    SQLExpr item = parseGroupByItem();

                    item.setParent(groupBy);
                    groupBy.addItem(item);

                    if (lexer.token()!= Token.COMMA&&lexer.token()!=Token.GT) {
                        break;
                    }

                    lexer.nextToken();
                }
            }

            if (lexer.token() == Token.WITH) {
                lexer.nextToken();
                acceptIdentifier("ROLLUP");

                groupBy.setWithRollUp(true);
            }

            if(JdbcConstants.MYSQL.equals(getDbType())
                    && lexer.token() == Token.DESC) {
                lexer.nextToken(); // skip
            }

            queryBlock.setGroupBy(groupBy);
        }
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
