package com.reporting.aggregation.flink.aggregators;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Pv;
import com.reporting.mocks.model.risks.aggregate.PvAggregate;
import com.reporting.mocks.model.risks.aggregate.RiskAggregationException;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;



public class PvRiskResultAggregator extends RiskResultAggregator<Pv, PvAggregate> {
    private static final Logger LOGGER = Logger.getLogger(PvRiskResultAggregator.class.getName());
    private static final long serialVersionUID = 4163974649863902376L;

    @Override
    protected PvAggregate aggregate(PvAggregate accumulator, RiskResult<Pv> riskResult) {
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
    protected PvAggregate createAccumulator(RiskResult<Pv> riskResult) {
        return new PvAggregate(riskResult.getRisk());
    }

    @Override
    protected String getKey(RiskResult<Pv> riskResult) {
        Pv pv = riskResult.getRisk();
        return pv.getUnderlying().getName();
    }

    @Override
    public void flatMap(RiskResult<Pv> riskResult, Collector<PvAggregate> collector) throws Exception {
        Gson gson = new Gson();
        for(String projectionKey : this.generateKeys(riskResult)) {
            String key = "/" + riskResult.getMarketEnvId().toString() + projectionKey;
            
            TypeInformation<PvAggregate> riskResultTypeInfo = TypeInformation.of(new TypeHint<PvAggregate>(){});
            TypeInformation<String> stringTypeInfo = TypeInformation.of(new TypeHint<String>(){});
            MapStateDescriptor<String, PvAggregate> risResultState = new MapStateDescriptor<>(key, stringTypeInfo, riskResultTypeInfo);
            StateTtlConfig riskResultStateConfig = StateTtlConfig
                    .newBuilder(Time.minutes(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            risResultState.enableTimeToLive(riskResultStateConfig);

            MapState<String, PvAggregate> aggregateStates = this.getRuntimeContext().getMapState(risResultState);

            PvAggregate riskAccu;
            String riskKey = this.getKey(riskResult);
            if (aggregateStates.contains(riskKey)) {
                riskAccu = aggregateStates.get(riskKey);
                this.aggregate(riskAccu, riskResult);
            } else {
                riskAccu = this.createAccumulator(riskResult);
                riskAccu.setBookName(projectionKey);
                aggregateStates.put(riskKey, riskAccu);
            }

            LOGGER.log(Level.INFO, "Aggregate for " + gson.toJson(riskAccu).toString());

            collector.collect(riskAccu);
        }

    }
}