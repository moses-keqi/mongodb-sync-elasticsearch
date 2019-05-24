package com.moses.sync.service;

import com.moses.sync.enums.MongoDdl;
import org.bson.Document;

/**
 * @author HanKeQi
 * @Description
 * @date 2019/4/19 3:58 PM
 **/

public interface ElasticsearchService {

    boolean elasticsearchDdl(MongoDdl ddl, String index, String mapping, String id, Document document) throws Exception;

    void screenMongoDmlEs(Document document);
}
