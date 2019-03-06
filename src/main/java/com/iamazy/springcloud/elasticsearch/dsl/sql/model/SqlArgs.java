package com.iamazy.springcloud.elasticsearch.dsl.sql.model;


import lombok.Getter;

/**
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
