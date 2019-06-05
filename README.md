基于Elasticsearch的Java Rest High Level Client的elasticsearch-sql查询组件
==========================

目录
---------------

 * [使用文档](#%E4%BD%BF%E7%94%A8%E6%96%87%E6%A1%A3)
  * [依赖](#%E4%BE%9D%E8%B5%96-elasticsearch-sql%E5%9C%B0%E5%9D%80)
  * [插件(isql)](#%E6%8F%92%E4%BB%B6isql)
      * [版本](#%E7%89%88%E6%9C%AC)
      * [安装](#%E5%AE%89%E8%A3%85)
      * [使用](#%E4%BD%BF%E7%94%A8)
  * [CHANGELOG](#changelog)
  * [版本](#%E7%89%88%E6%9C%AC-1)
  * [感谢](#%E6%84%9F%E8%B0%A2)
  * [介绍](#%E4%BB%8B%E7%BB%8D)
  * [特点](#%E7%89%B9%E7%82%B9)
  * [功能点](#%E5%8A%9F%E8%83%BD%E7%82%B9)
  * [测试用例](#%E6%B5%8B%E8%AF%95%E7%94%A8%E4%BE%8B)


使用文档
--------------------
[elasticsearch-sql-wiki](https://github.com/iamazy/elasticsearch-sql/wiki)

依赖 [elasticsearch-sql地址](https://search.maven.org/artifact/io.github.iamazy.elasticsearch.dsl/elasticsearch-sql/6.6.2/jar)
--------------------
```xml
<dependency>
  <groupId>io.github.iamazy.elasticsearch.dsl</groupId>
  <artifactId>elasticsearch-sql</artifactId>
  <version>7.1.1.1</version>
</dependency>
```

插件(isql)
--------------------
#### 版本

| elasticsearch version | latest version | remark | isql version | 
| ---- | ---- | ---- | ---- | 
| 6.x | 6.6.0 | | 6.6.0.1 |
| 6.x | 6.6.1 | | 6.6.1.1 |
| 6.x | 6.6.2 | | 6.6.2.1 |
| 6.x | 6.7.0 | | 6.7.0.1 |
| 6.x | 6.7.1 | | 6.7.1.1 |
| 7.x | 7.0.0 | | 7.0.0.2 |
| 7.x | 7.0.1 | | 7.0.1.1 |
| 7.x | 7.1.0 | | 7.1.0.1 |
| 7.x | 7.1.1 | | 7.1.1.1 |

#### 安装

Elasticsearch {6.x,7.x}
```
./bin/elasticsearch-plugin install https://github.com/iamazy/elasticsearch-sql/releases/download/{isql-version}/elasticsearch-sql-plugin-{isql-version}.zip
```

#### 使用

##### 1. 使用sql语句直接查询elasticsearch里面的数据集
```
POST _isql
{
    "sql":"select * from fruit"
}
```
###### 语法：desc [index]
```
POST _isql
{
    "sql":"desc fruit"
}
```
###### 语法：desc [index]/[field]
```
POST _isql
{
    "sql":"desc fruit/name"
}
```

##### 2. 将sql解析成elasticsearch的dsl
```
POST _isql/_explain
{
    "sql":"select * from fruit"
}
```

CHANGELOG
--------------------

2019-3-6：修复原版Nested类型的nested path识别错误的问题<br/>
2019-3-7：删除了大部分无用的代码，添加了geo_distance聚类方法<br/>
2019-3-25: 聚类使用递归实现添加多层嵌套聚类方式([>]表示嵌套聚类[,]表示同级聚类),具体用法见test目录<br/>
2019-3-26: 添加scroll id深度分页<br/>
2019-3-28: 更新nested功能,支持双层嵌套类型（再多就要考虑数据结构是否合理了）<br/>
2019-4-8: 添加高亮显示<br/>
2019-4-11: 添加Function Score<br/>
2019-4-24: 将elasticsearch-sql添加为elasticsearch插件<br/>
2019-4-28: 添加like not like 查询<br/>
2019-5-5: 添加desc语法获取index(或者index/field)的mapping,无法直接获取实际的mapping,必须结合restClient使用,且desc后面只能加一个index的名称</br>
2019-5-8: 添加excludes字段（在字段前加[^]）<br/>
2019-6-5: 解决了索引名带中划线【-】报错的bug <br/> 

[CHANGELOG](https://github.com/iamazy/elasticsearch-sql/edit/master/CHANGELOG)

版本
---------------------
|elasticsearch-sql|es version|
|----|-----|
|master|6.6.0|
|master|6.6.2|
|master|6.7.0|
|master|6.7.1|
|master|7.0.0|
|master|7.0.1|


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
##### 3）方便鉴权
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
我是向[https://github.com/NLPchina/elasticsearch-sql](https://github.com/NLPchina/elasticsearch-sql)的开发团队看齐的，功能点会慢慢的一点一点的添加的
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
- [x] SQL Max
- [x] SQL Min
- [x] SQL Sum
- [x] SQL Avg
- [x] SQL > & < & >= & <=

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
- [x] ES Range(Number,Date)

#### `elasticsearch-sql` 新增的功能
- [x] ES MatchAll
- [x] ES MatchPhrase
- [x] ES MatchPhrasePrefix
- [x] ES DeleteByQuery
- [x] ES Cardinality (目前不支持Script的方式)
- [x] ES TopHits
- [x] ES Nested (elasticsearch-query-toolkit中nested表达方式不合理，已修正)
- [x] ES GeoDistance
- [x] 支持嵌套深层聚类
- [x] ES Scroll Id
- [x] ES 支持双层嵌套查询（nested(nested)）现在以及以后也不会支持三层以上的嵌套查询
- [x] ES Highlighter
- [x] ES Boosting
- [x] ES Function Score
- [x] SQL Like
- [x] SQL Desc
- [x] ES Excludes

#### 未来将要添加的功能
- [x] ES Highlighter
- [ ] elasticsearch-sql[NLPChina]组件中我未添加的功能!!!

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
    String sql="DELETE from fruits where match_all() limit 1100";
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

### 5. Nested
 为了表征**nested path**这个属性,采用 **$** 符号指明 <br/>
nested path必须以 **$** 在**为nested类型的属性之前**结尾（非常重要）<br/>
🐖：一个嵌套表达式最多包含2个$符号

<font color="red"><b>重要:</b></font>以`product`的`apple`为例，`apple`为`nested`类型，则查询时的**nested path**应该为`product.apple`
以下两种写法均**正确**
```
product$apple.name
```
下面这几种写法**错误**
```
$product$apple.name
product.apple$name
$product.apple$name
$product$apple$name
product$apple$name
```
Nested结构参照
```
"product" : {
    "properties" : {
        "apple" : {
            "type" : "nested",
            "properties" : {
                "name" : {
                "type" : "text"
                },
                "price" : {
                "type" : "double"
                }
            }
        }
    }
}
```

🌹其余的请去test目录下找吧

欢迎大家提issue




















