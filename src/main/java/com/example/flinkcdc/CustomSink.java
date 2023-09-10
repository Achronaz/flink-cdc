package com.example.flinkcdc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CustomSink extends RichSinkFunction<String> {
    @Override
    public void invoke(String json, Context ctx) throws Exception{
        log.info(">>> {}",json);
    }
    @Override
    public void open(Configuration parameters) throws Exception {

    }
    @Override
    public void close() throws Exception {

    }

}
