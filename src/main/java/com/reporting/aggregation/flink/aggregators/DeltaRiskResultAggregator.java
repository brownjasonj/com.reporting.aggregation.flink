package com.reporting.aggregation.flink.aggregators;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Delta;
import com.reporting.mocks.model.risks.aggregate.DeltaAggregate;
import com.reporting.mocks.model.risks.aggregate.RiskAggregationException;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

public class DeltaRiskResultAggregator extends RiskResultAggregator<Delta, DeltaAggregate> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(DeltaRiskResultAggregator.class.getName());

    @Override
    protected DeltaAggregate aggregate(DeltaAggregate accumulator, RiskResult<Delta> riskResult) {
        try {
            if (riskResult.isDeleteEvent())
                accumulator.add(riskResult.getRisk());
            else
                accumulator.subtract(riskResult.getRisk());
        } catch (RiskAggregationException rae) {
            LOGGER.log(Level.WARNING, "Aggregation error, Risk being aggregated is from a difference caluclation context");
        }
        return accumulator;
    }

    @Override
    protected DeltaAggregate createAccumulator(RiskResult<Delta> riskResult) {
        return new DeltaAggregate(riskResult.getRisk());
    }

    @Override
    protected String getKey(RiskResult<Delta> riskResult) {
        Delta delta = riskResult.getRisk();
        return delta.getUnderlying().getName();
    }

    @Override
    public void flatMap(RiskResult<Delta> riskResult, Collector<DeltaAggregate> collector) throws Exception {
        Gson gson = new Gson();
        for(String projectionKey : this.generateKeys(riskResult)) {
            String key = riskResult.getMarketEnvId().toString() + "/" + projectionKey;
            
            TypeInformation<DeltaAggregate> riskResultTypeInfo = TypeInformation.of(new TypeHint<DeltaAggregate>(){});
            TypeInformation<String> stringTypeInfo = TypeInformation.of(new TypeHint<String>(){});
            MapStateDescriptor<String, DeltaAggregate> risResultState = new MapStateDescriptor<>(key, stringTypeInfo, riskResultTypeInfo);
            StateTtlConfig riskResultStateConfig = StateTtlConfig
                    .newBuilder(Time.minutes(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            risResultState.enableTimeToLive(riskResultStateConfig);

            MapState<String, DeltaAggregate> aggregateStates = this.getRuntimeContext().getMapState(risResultState);

            DeltaAggregate riskAccu;
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