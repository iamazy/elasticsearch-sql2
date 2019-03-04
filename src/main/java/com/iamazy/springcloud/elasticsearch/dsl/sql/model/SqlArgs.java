package com.iamazy.springcloud.elasticsearch.dsl.sql.model;


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
public class SqlArgs {
    private  Object[] args;

    public SqlArgs(Object[] args){
        this.args=args;
        ensureAllNotNull();
    }

    private void ensureAllNotNull(){
        if(args==null){
            throw new IllegalArgumentException("Sql args is null");
        }
        for(Object arg:args){
            if(arg==null){
                throw new IllegalArgumentException("The sql args have one or more null arg");
            }
        }
    }

    public int count(){
        return args.length;
    }

    public Object get(int index){
        if(index>=count()){
            throw new IndexOutOfBoundsException(String.format("index[%s] can't bigger than args length[%s]",index,count()));
        }
        return args[index];
    }

}
