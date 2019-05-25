package com.reporting.aggregation.flink.aggregators;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Risk;
import com.reporting.mocks.model.risks.aggregate.RiskAggregate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

public abstract class RiskResultAggregator<R extends Risk, RA extends RiskAggregate>
        extends RichFlatMapFunction<RiskResult<R>, RA> {

    private static final long serialVersionUID = 1L;

    protected Gson gson;
    protected abstract RA aggregate(RA accumulator, RiskResult<R> value);
    protected abstract RA createAccumulator(RiskResult<R> riskResult);
    protected abstract String getKey(RiskResult<R> riskResult);

    protected RiskResultAggregator() {
        this.gson = new Gson();
    }

    protected List<String> generateKeys(RiskResult<R> riskResult) {
        ArrayList<String> projectionSet = new ArrayList<>();
        String[] bookElements = riskResult.getRisk().getBookName().split(":");
        String riskType = riskResult.getRisk().getRiskType().toString();

        StringWriter keyElement = new StringWriter();
        for(String bookElement : bookElements) {
            keyElement.write(bookElement);
            keyElement.append('/');
            keyElement.write(riskType);
            projectionSet.add(keyElement.toString());
        }
        return projectionSet;
    }

    @Override
    public void flatMap(RiskResult<R> riskResult, Collector<RA> collector) throws Exception {
        for(String projectionKey : this.generateKeys(riskResult)) {
            String key = riskResult.getMarketEnvId().toString() + "/" + projectionKey;
            
            TypeInformation<RA> riskResultTypeInfo = TypeInformation.of(new TypeHint<RA>(){});
            TypeInformation<String> stringTypeInfo = TypeInformation.of(new TypeHint<String>(){});
            MapStateDescriptor<String, RA> risResultState = new MapStateDescriptor<>(key, stringTypeInfo, riskResultTypeInfo);
            StateTtlConfig riskResultStateConfig = StateTtlConfig
                    .newBuilder(Time.minutes(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            risResultState.enableTimeToLive(riskResultStateConfig);

            MapState<String, RA> aggregateStates = this.getRuntimeContext().getMapState(risResultState);

            RA riskAccu;
            String riskKey = this.getKey(riskResult);
            if (aggregateStates.contains(riskKey)) {
                riskAccu = aggregateStates.get(riskKey);
                this.aggregate(riskAccu, riskResult);
            } else {
                riskAccu = this.createAccumulator(riskResult);
                aggregateStates.put(riskKey, riskAccu);
            }

            System.out.printf("Aggregate for %s = %f, %f\n", key, gson.toJson(riskAccu).toString());

            collector.collect(riskAccu);
        }

    }
}