package com.moses.sync;

import com.moses.sync.source.MongoOplogSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author HanKeQi
 * @Description
 * @date 2019/5/24 10:12 AM
 **/
@EnableScheduling
@SpringBootApplication
public class MongoSyncElasticsearchApplication implements CommandLineRunner {

    @Autowired
    private MongoOplogSource mongoOplogSource;

    public static void main(String[] args) {
        SpringApplication.run(MongoSyncElasticsearchApplication.class, args);
    }

    @Override
    public void run(String... args){
        mongoOplogSource.run();
    }
}
