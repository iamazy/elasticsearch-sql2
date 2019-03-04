 基于Elasticsearch的Java Rest High Level Client的elasticsearch-sql查询组件
==========================

版本
---------------------
|elasticsearch-sql|es version|
|----|-----|
|master|6.6.0|

感谢
--------------------------
首先感谢`elasticsearch-query-toolkit`的作者`gitchennan`,elasticsearch-sql基于`gitchennan`的`elasticsearch-query-toolkit`，并在其中稍作修改和添加部分功能，再次感谢`gitchennan`哈哈哈哈哈<br/>
`gitchennan`的github地址:[elasticsearch-query-toolkit](https://github.com/gitchennan/elasticsearch-query-toolkit)


介绍
-------------------------
elasticsearch-sql是一个基于sql查询的elasticsearch编程工具包，支持sql生成elasticsearch dsl,去掉了`elasticsearch-query-toolkit`中与Spring,Mybatis
集成的部分，有需要的话请参照`elasticsearch-query-toolkit`<br/>

特点
----------------------
##### 1）elasticsearch-sql是基于Java Rest High Level Client构建elasticsearch查询的，支持elasticsearch原生rest client调用以及第三方http请求
##### 2）基于 `alibaba`的Druid数据连接池的SqlParser组件，解析sql速度快，自定义解析规则更方便
##### 3）支持鉴权
抛弃elasticsearch传统的transport连接方式改用rest high level连接方式不仅仅是因为官方建议，而是在结合x-pack组件进行鉴权的时候更加方便
本人不知道在transport连接方式中如何复用transport client进行多用户的搜索请求
下面是官网的一段代码
```java
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
...

TransportClient client = new PreBuiltXPackTransportClient(Settings.builder()
        .put("cluster.name", "myClusterName")
        .put("xpack.security.user", "transport_client_user:x-pack-test-password")
        ...
        .build())
    .addTransportAddress(new TransportAddress("localhost", 9300))
    .addTransportAddress(new TransportAddress("localhost", 9301));
```
每一个transport client都需要将用户名和密码写死在配置里面，如果要使用多用户进行请求的话，就不得不建立多个transport client连接，这种方式感觉很傻，尽管我之前写过用common-pool2组件管理transport client连接，但是一想到以后会有成千上万的用户访问就要建立成千上万的连接，想想都要爆炸💥
<br/>但是rest high level client就没有这个问题，它通过`RequestOptions`携带用户的token信息进行搜索请求，不同的用户搜索只需要分配不同的`RequestOptions`就行了
```java
private static RequestOptions requestOptions(String token) {
        //设置允许返回的最大字节数
        HttpAsyncResponseConsumerFactory responseConsumerFactory = new HttpAsyncResponseConsumerFactory
                .HeapBufferedResponseConsumerFactory(Integer.MAX_VALUE);
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setHttpAsyncResponseConsumerFactory(responseConsumerFactory);
        builder.addHeader("Authorization", "Basic " + token);
        return builder.build();
}

public Map<String, Object> get(String cluster,String index,String type, String id, String routing, String token) throws IOException {
        GetRequest getRequest = new GetRequest(index, type, id).routing(routing);
        GetResponse getResponse = getRestHighLevelClient(cluster).get(getRequest, requestOptions(token));
        return getResponse.getSourceAsMap();
}
```
简单又方便，简直不要太棒了好吧<br/>
<font size="10">🐷</font>也许是我的使用问题，如果有人知道如何使用transport client进行多用户的搜索请求，麻烦告诉我一下，我还挺好奇的

功能点
------------
我本人是向[https://github.com/NLPchina/elasticsearch-sql](https://github.com/NLPchina/elasticsearch-sql)的开发团队看齐的，功能点会慢慢的一点一点的添加的
#### `elasticsearch-query-toolkit`已有的功能
- [x] SQL Select  
- [x] SQL Where  
- [x] SQL Order by (Asc & Desc)
- [x] SQL Group by (Terms & Range)
- [x] SQL And & Or
- [x] SQL In
- [x] SQL Between And
- [x] SQL Is
- [x] SQL Not
- [x] SQL Null
- [x] SQL Nvl

- [x] ES FullText
- [x] ES Match
- [x] ES MultiMatch
- [x] ES QueryString
- [x] ES SimpleQueryString
- [x] ES HasParent
- [x] ES HasChild
- [x] ES Join
- [x] ES Script
- [x] ES Fuzzy
- [x] ES Prefix
- [x] ES Regex
- [x] ES Term
- [x] ES Wildcard
- [x] ES Routing
- [x] ES Nested
- [x] ES Include[fields]
- [x] ES From
- [x] ES Size

#### `elasticsearch-sql` 新增的功能
- [x] ES MatchAll
- [x] ES MatchPhrase
- [x] ES MatchPhrasePrefix
- [x] ES DeleteByQuery

#### 未来将要添加的功能
- [] ES Highlighter
- [] elasticsearch-sql[NLPChina]组件中我未添加的功能!!!

<font size="10">☀️</font>未来的想法是将功能完善的跟NLPChina团队一样多嘻嘻

测试用例
---------
提供几个SQL转DSL的例子(在源码test文件夹里)，其他部分你们需要去[elasticsearch-query-toolkit](https://github.com/gitchennan/elasticsearch-query-toolkit)了解，或者自己看源码(推荐，原作者的代码很优秀)


### 1. Match
 ```java
@Test
public void testParseFromMethodSource(){
    String sql="select * from fruits where match(name,'apple','prefix_length:21') and term(color,'red')";
    ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
    ElasticSqlParseResult parseResult = sql2DslParser.parse(sql,new String[]{"name","color"});
    System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
}
```
```json
{
  "from" : 0,
  "size" : 15,
  "query" : {
    "bool" : {
      "filter" : [ {
        "bool" : {
          "must" : [ {
            "match" : {
              "name" : {
                "query" : "apple",
                "operator" : "OR",
                "prefix_length" : 21,
                "max_expansions" : 50,
                "fuzzy_transpositions" : true,
                "lenient" : false,
                "zero_terms_query" : "NONE",
                "auto_generate_synonyms_phrase_query" : true,
                "boost" : 1.0
              }
            }
          }, {
            "term" : {
              "color" : {
                "value" : "red",
                "boost" : 1.0
              }
            }
          } ],
          "adjust_pure_negative" : true,
          "boost" : 1.0
        }
      } ],
      "adjust_pure_negative" : true,
      "boost" : 1.0
    }
  }
}
```

### 2. MatchPhrase,Term,Limit
```java
@Test
public void testParseLimit(){
    String sql="select * from fruits where match_phrase(name,'apple') and term(color,'red') limit 2,9";
    ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
    ElasticSqlParseResult parseResult = sql2DslParser.parse(sql,new String[]{"name","color"});
    System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
}
```
```json
{
  "from" : 2,
  "size" : 9,
  "query" : {
    "bool" : {
      "filter" : [ {
        "bool" : {
          "must" : [ {
            "match_phrase" : {
              "name" : {
                "query" : "apple",
                "slop" : 0,
                "zero_terms_query" : "NONE",
                "boost" : 1.0
              }
            }
          }, {
            "term" : {
              "color" : {
                "value" : "red",
                "boost" : 1.0
              }
            }
          } ],
          "adjust_pure_negative" : true,
          "boost" : 1.0
        }
      } ],
      "adjust_pure_negative" : true,
      "boost" : 1.0
    }
  }
}
```
🐷Term(a,b) 也可以在SQL中直接写成a='b'

###  3）Terms Agg
```java
@Test
public void testParseTermsAgg(){
    String sql="select * from fruits where name is not null and color is not null group by terms(weight,5000),terms(color,600)";
    ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
    ElasticSqlParseResult parseResult = sql2DslParser.parse(sql,new String[]{"name","color"});
    System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
}
```
```json

  "from" : 0,
  "size" : 15,
  "query" : {
    "bool" : {
      "filter" : [ {
        "bool" : {
          "must" : [ {
            "exists" : {
              "field" : "name",
              "boost" : 1.0
            }
          }, {
            "exists" : {
              "field" : "color",
              "boost" : 1.0
            }
          } ],
          "adjust_pure_negative" : true,
          "boost" : 1.0
        }
      } ],
      "adjust_pure_negative" : true,
      "boost" : 1.0
    }
  },
  "aggregations" : {
    "weight" : {
      "terms" : {
        "field" : "weight",
        "size" : 5000,
        "shard_size" : 10000,
        "min_doc_count" : 1,
        "shard_min_doc_count" : 1,
        "show_term_doc_count_error" : false,
        "order" : [ {
          "_count" : "desc"
        }, {
          "_key" : "asc"
        } ]
      }
    },
    "color" : {
      "terms" : {
        "field" : "color",
        "size" : 600,
        "shard_size" : 1200,
        "min_doc_count" : 1,
        "shard_min_doc_count" : 1,
        "show_term_doc_count_error" : false,
        "order" : [ {
          "_count" : "desc"
        }, {
          "_key" : "asc"
        } ]
      }
    }
  }
}
```

### 4. Delete
```java
public static void main(String[] args) {
    String sql="DELETE from device_search where match_all() limit 1100";
    ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
    ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql, new String[]{"port"});
    System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toDelRequest().getSearchRequest()));
}
```
```json
{
  "size" : 1000,
  "query" : {
    "bool" : {
      "must" : [ {
        "match_all" : {
          "boost" : 1.0
        }
      } ],
      "adjust_pure_negative" : true,
      "boost" : 1.0
    }
  },
  "_source" : false
}
```
🐷 DSL里的size=1000和Java中的`limit 1100`含义不一样
size=1000 是DeleteByQueryRequest中的SearchRequest的Size，默认为1000
limit 1100 设置的是DeleteByQueryRequest的Size，只是在DSL中没有显示

🌹其余的请区test目录下去找吧哈哈，如果大家喜欢就给个star吧，多多fork，算是对我的激励吧👿























