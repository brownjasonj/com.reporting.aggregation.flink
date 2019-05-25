package com.reporting.aggregation.flink.jobs;

import com.reporting.aggregation.flink.aggregators.DeltaRiskResultAggregator;
import com.reporting.aggregation.flink.filters.RiskResultFilterByRiskType;
import com.reporting.aggregation.flink.selectors.RiskResultMarketIdKeySelector;
import com.reporting.aggregation.flink.serialization.RiskAggregateSerializationSchema;
import com.reporting.aggregation.flink.serialization.RiskResultDeserilaizationSchema;
import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Delta;
import com.reporting.mocks.model.risks.aggregate.DeltaAggregate;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeltaAggregationJob {
    private static final Logger LOGGER = Logger.getLogger(DeltaAggregationJob.class.getName());

    private String BOOTSTRAPSERVER = "localhost:9092";
    private String RISKRESULT = "IntraDayRiskTick";
    private String RISKAGGREGATIONRESULTS = "AggregateResults";

    private Properties kafkaProperties;

    public DeltaAggregationJob() {
        this.kafkaProperties = new Properties();
        this.kafkaProperties.setProperty("bootstrap.servers", this.BOOTSTRAPSERVER);
        this.kafkaProperties.setProperty("group.id", "test1");
    }

    public void go() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        FlinkKafkaConsumer010<RiskResult<Delta>> consumer = new FlinkKafkaConsumer010<>(this.RISKRESULT,
                new RiskResultDeserilaizationSchema<Delta>(), this.kafkaProperties);
        FlinkKafkaProducer010<DeltaAggregate> riskAggregateProducer = new FlinkKafkaProducer010<>(this.BOOTSTRAPSERVER, // broker
                this.RISKAGGREGATIONRESULTS, // target topic
                new RiskAggregateSerializationSchema<DeltaAggregate>()); // serialization schema

        consumer.setCommitOffsetsOnCheckpoints(false);

        // versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
        // this method is not available for earlier Kafka versions
        riskAggregateProducer.setWriteTimestampToKafka(true);

        DataStream<RiskResult<Delta>> stream = env
                .addSource(consumer)
                .filter(new RiskResultFilterByRiskType<Delta>(Delta.class));

        stream.keyBy(new RiskResultMarketIdKeySelector<>())
            .flatMap(new DeltaRiskResultAggregator())
            .addSink(riskAggregateProducer)
            .setParallelism(1);

        LOGGER.log( Level.FINE, "Connecting to Kafka topic: {}",this.RISKRESULT);


        env.execute("DeltaAggregationJob");
    }

    public static void main(String[] args) throws Exception {
        DeltaAggregationJob job = new DeltaAggregationJob();
        job.go();
    }

}