package com.reporting.aggregation.flink.serialization;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.*;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class RiskResultDeserilaizationSchema<R extends Risk> extends AbstractDeserializationSchema<RiskResult<R>> {

    private static final long serialVersionUID = -1788833843598168455L;

    @Override
    public RiskResult<R> deserialize(byte[] message) throws IOException {
        Gson gson = new Gson();
        String stringMessage = new String(message);
        RiskResult<? extends Risk> rr =  gson.fromJson(stringMessage, new TypeToken<RiskResult<? extends Risk>>(){

            private static final long serialVersionUID = 7578530302901661311L;
        }.getType());
        switch (rr.getRisk().getRiskType()) {
            case PV:
                return gson.fromJson(stringMessage, new TypeToken<RiskResult<Pv>>(){
                private static final long serialVersionUID = 1L;
            }.getType());
            case DELTA:
                return gson.fromJson(stringMessage, new TypeToken<RiskResult<Delta>>(){
                private static final long serialVersionUID = 1L;
            }.getType());
            case GAMMA:
                return gson.fromJson(stringMessage, new TypeToken<RiskResult<Gamma>>(){
                private static final long serialVersionUID = 1L;
            }.getType());
            default:
                return gson.fromJson(stringMessage, new TypeToken<RiskResult<Vega>>(){
                private static final long serialVersionUID = 3086695730369026959L;
            }.getType());
        }
    }
}
