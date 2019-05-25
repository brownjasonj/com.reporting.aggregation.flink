package com.reporting.aggregation.flink.serialization;

import com.google.gson.Gson;
import com.reporting.mocks.model.risks.aggregate.RiskAggregate;

import org.apache.flink.api.common.serialization.SerializationSchema;

public class RiskAggregateSerializationSchema<R extends RiskAggregate> implements SerializationSchema<R> {
    private static final long serialVersionUID = 1169577268460193274L;

    @Override
    public byte[] serialize(R element) {
        Gson gson = new Gson();
        return gson.toJson(element).getBytes();
    }
}