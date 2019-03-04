package com.iamazy.springcloud.elasticsearch.dsl.configuration;

import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


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
 * @date 2019/2/21
 * @descrition
 **/
@Configuration
public class ElasticSql2DslConfiguration {

    @Bean(name = "elasticSql2DslParser")
    public ElasticSql2DslParser elasticSql2DslParser(){
        return new ElasticSql2DslParser();
    }
}
