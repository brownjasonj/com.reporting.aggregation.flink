package com.reporting.aggregation.flink.jobs;

import com.reporting.aggregation.flink.aggregators.PvRiskResultAggregator;
import com.reporting.aggregation.flink.filters.RiskResultFilterByRiskType;
import com.reporting.aggregation.flink.selectors.RiskResultMarketIdKeySelector;
import com.reporting.aggregation.flink.serialization.RiskAggregateSerializationSchema;
import com.reporting.aggregation.flink.serialization.RiskResultDeserilaizationSchema;
import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Pv;
import com.reporting.mocks.model.risks.aggregate.PvAggregate;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PvAggregationJob {
    private static final Logger LOGGER = Logger.getLogger(PvAggregationJob.class.getName());

    private String BOOTSTRAPSERVER = "localhost:9092";
    private String RISKRESULT = "IntraDayRiskTick";
    private String RISKAGGREGATIONRESULTS = "AggregateResults";

    private Properties kafkaProperties;

    public PvAggregationJob() {
        this.kafkaProperties = new Properties();
        this.kafkaProperties.setProperty("bootstrap.servers", this.BOOTSTRAPSERVER);
        this.kafkaProperties.setProperty("group.id", "test1");
    }

    public void go() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setBufferTimeout(-1);

        FlinkKafkaConsumer010<RiskResult<Pv>> consumer = new FlinkKafkaConsumer010<>(this.RISKRESULT,
                new RiskResultDeserilaizationSchema<Pv>(), this.kafkaProperties);
        FlinkKafkaProducer010<PvAggregate> riskAggregateProducer = new FlinkKafkaProducer010<>(this.BOOTSTRAPSERVER, // broker
                                                                                                               // list
                this.RISKAGGREGATIONRESULTS, // target topic
                new RiskAggregateSerializationSchema<PvAggregate>()); // serialization schema

        consumer.setCommitOffsetsOnCheckpoints(false);

        // versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
        // this method is not available for earlier Kafka versions
        riskAggregateProducer.setWriteTimestampToKafka(true);

        DataStream<RiskResult<Pv>> stream = env
                .addSource(consumer)
                .filter(new RiskResultFilterByRiskType<Pv>(Pv.class));

        stream.keyBy(new RiskResultMarketIdKeySelector<>())
            .flatMap(new PvRiskResultAggregator())
            .addSink(riskAggregateProducer)
            .setParallelism(1);

        LOGGER.log( Level.FINE, "Connecting to Kafka topic: {}",this.RISKRESULT);


        env.execute("PvAggregationJob");
    }

    public static void main(String[] args) throws Exception {
        PvAggregationJob job = new PvAggregationJob();
        job.go();
    }

}