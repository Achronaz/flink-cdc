package com.example.flinkcdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.springframework.stereotype.Component;
import org.apache.flink.configuration.Configuration;

import javax.annotation.PostConstruct;

@Component
public class FlinkCdc {

    @PostConstruct
    public void init() throws Exception {
        start();
    }

    public static void start() throws Exception {
        System.setProperty("logger.flink.level","INFO");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3307)
                .databaseList("olnpmt2") // set captured database
                .tableList("olnpmt2.invitation_code") // set captured table
                .username("root")
                .password("abcd1234")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .includeSchemaChanges(true)
                .build();

        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.enableCheckpointing(10000);

        DataStreamSink<String> sink = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                        .addSink(new CustomSink());
        env.execute();
    }
}
