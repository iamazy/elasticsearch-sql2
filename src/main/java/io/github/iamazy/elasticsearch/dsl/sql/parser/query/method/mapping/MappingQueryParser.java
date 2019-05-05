package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.mapping;


import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;

/**
 * @author iamazy
 * @date 2019/5/5
 * @descrition
 **/
public class MappingQueryParser {

    public static Object parse(String sql) {
        String[] descItems = StringUtils.split(sql, " ");
        GetFieldMappingsRequest getFieldMappingsRequest = new GetFieldMappingsRequest();
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        if (descItems.length == 2) {
            String[] items = descItems[1].split("/");
            switch (items.length) {
                case 2: {
                    getFieldMappingsRequest.indices(items[0]);
                    getFieldMappingsRequest.fields(items[1]);
                    return getFieldMappingsRequest;
                }
                case 1:
                default: {
                    getMappingsRequest.indices(items[0]);
                    return getMappingsRequest;
                }
            }
        } else {
            throw new ElasticSql2DslException("[syntax error] desc must have table name or table/field name");
        }
    }

}
