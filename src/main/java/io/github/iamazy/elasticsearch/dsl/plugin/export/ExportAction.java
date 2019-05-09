package io.github.iamazy.elasticsearch.dsl.plugin.export;

import io.github.iamazy.elasticsearch.dsl.cons.CoreConstants;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;


/**
 * @author iamazy
 * @date 2019/5/9
 * @descrition
 **/
@Slf4j
public class ExportAction {

    public static Map<String,Object> EXPORT_TASK_MAP=new CopyOnWriteHashMap<>();

    public void export(Client client, SearchRequest searchRequest,long size) throws IOException {
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        searchRequest.scroll(scroll);
        SearchResponse searchResponse = client.search(searchRequest).actionGet();
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        String taskId=CoreConstants.OBJECT_ID_GENERATOR.generate().toString();
        File file=new File(taskId+".json");
        if(file.exists()){
            throw new IOException("任务Id已存在!!!");
        }else{
            if(!file.createNewFile()){
                throw new IOException("创建文件失败!!!");
            }
        }
        long total=searchResponse.getHits().getTotalHits().value;
        long currentSize=0L;

        Map<String,Object> currentTaskInfo=new HashMap<>(0);
        currentTaskInfo.put("taskId",taskId);
        currentTaskInfo.put("total",total);
        currentTaskInfo.put("indices",searchRequest.indices());
        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = client.searchScroll(scrollRequest).actionGet();
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
            for(SearchHit hit:searchHits){
                Files.write(Paths.get(file.toURI()),hit.getSourceAsString().getBytes(), StandardOpenOption.APPEND);
            }
            double percent=((double) currentSize)/(double) total;
            currentTaskInfo.put("percent",percent);
            currentSize+=searchHits.length;
            EXPORT_TASK_MAP.put(taskId,currentTaskInfo);
            if(size<currentSize){
                break;
            }
        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest).actionGet();
        boolean succeeded = clearScrollResponse.isSucceeded();
        if(succeeded) {
            log.info(taskId + ".json导出成功!!!");
        }
    }
}
