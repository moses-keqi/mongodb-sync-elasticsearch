package com.moses.sync.service.impl;

import com.moses.sync.enums.MongoDdl;
import com.moses.sync.service.ElasticsearchService;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;


/**
 * @author HanKeQi
 * @Description  总调度中心
 * @date 2019/4/19 3:58 PM
 **/
@Service
@Slf4j
public class ElasticsearchServiceImpl implements ElasticsearchService {

    @Autowired
    private TransportClient transportClient;

    @Override
    public boolean elasticsearchDdl(MongoDdl ddl, String index, String mapping, String id, Document document) throws Exception {
        switch (ddl) {
            case i: // insert operation
                transportClient.prepareIndex(index, mapping, id)
                        .setSource(document).get();
                return true;
            case u: // update opertaion
                //此处一定要先查，在覆盖, 如果使用update查询很艰难
                Document updateSet = (Document)document.get("$set");
                Document document1 = elasticsearchSelect(index, mapping, id);
                if (document1 == null){
                    log.info("not fund index = {}, mapping = {}, id = {} update error", index, mapping, id);
                    return false;
                }
                document1.putAll(updateSet);
                transportClient.prepareIndex(index, mapping, id).setSource(document1).get();
                return true;
            case d: // delete operation
                transportClient.prepareDelete(index, mapping, id).get();
                return true;
        }
        return false;
    }

    @Override
    public void screenMongoDmlEs(Document document) {

        final String operation = document.getString("op"); // get type of CRUD operation
        final Document o = (Document) document.get("o"); // this is where object lives
        final String ns = (String) document.get("ns");

        String[] arr = ns.split("\\.");
        String index = arr[0];
        String mapping = arr[1];
        MongoDdl ddl = MongoDdl.valueOf(operation);
//            String batchId = String.valueOf(o.get("batchId"));
        String _id = String.valueOf(o.get("_id"));
        //主键_id与elasticsearch _id冲突
        o.remove("_id"); // 会自动转换成对象id
        o.remove("_class");
        index = String.format("%s_%s", index, mapping) ;
        try {
            //如果是修改，调用的是O2 内容存在o中
            if (MongoDdl.u == ddl){
                final Document o2 = (Document) document.get("o2");
                _id = String.valueOf(o2.get("_id"));
            }
            //log.info("operation to elasticsearch ddl = {}, index = {}, table = {}", ddl.getName(), index, mapping);
            elasticsearchDdl(ddl, index, mapping, _id, o);
        }catch (Exception e){
            log.error(" operation to elasticsearch error send mail manager ...ddl = {}, index = {}, table = {}, id = {}", ddl.getName(), index, mapping, _id);
            e.printStackTrace();
        }

    }

    private Document elasticsearchSelect(String index, String mapping, String id) {
        GetResponse getFields = null;
        try{
            getFields = transportClient.prepareGet(index, mapping, id).get();
        }catch (Exception e){
            log.error("error not fund index = {}, mapping = {}, id = {}, e = {}", index, mapping, id, e.getMessage());
            try {
                getFields = transportClient.prepareGet(index, mapping, id).get();
            }catch (Exception e1){
                return null;
            }
        }
        if (!getFields.isExists()){
            return  null;
        }
        Map<String, Object> sourceAsMap = getFields.getSourceAsMap();
        Document document = new Document();
        document.putAll(sourceAsMap);
        return document;
    }
}
