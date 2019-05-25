package com.reporting.aggregation.flink.aggregators;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Gamma;
import com.reporting.mocks.model.risks.aggregate.GammaAggregate;
import com.reporting.mocks.model.risks.aggregate.RiskAggregationException;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

public class GammaRiskResultAggregator extends RiskResultAggregator<Gamma, GammaAggregate> {
    private static final long serialVersionUID = 2213176606626280233L;
    private static final Logger LOGGER = Logger.getLogger(GammaRiskResultAggregator.class.getName());


    @Override
    protected GammaAggregate aggregate(GammaAggregate accumulator, RiskResult<Gamma> riskResult) {
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
    protected GammaAggregate createAccumulator(RiskResult<Gamma> riskResult) {
        return new GammaAggregate(riskResult.getRisk());
    }

    @Override
    protected String getKey(RiskResult<Gamma> riskResult) {
        Gamma gamma = riskResult.getRisk();
        return gamma.getUnderlying().getName();
    }
    
    @Override
    public void flatMap(RiskResult<Gamma> riskResult, Collector<GammaAggregate> collector) throws Exception {
        Gson gson = new Gson();
        for(String projectionKey : this.generateKeys(riskResult)) {
            String key = riskResult.getMarketEnvId().toString() + "/" + projectionKey;
            
            TypeInformation<GammaAggregate> riskResultTypeInfo = TypeInformation.of(new TypeHint<GammaAggregate>(){});
            TypeInformation<String> stringTypeInfo = TypeInformation.of(new TypeHint<String>(){});
            MapStateDescriptor<String, GammaAggregate> risResultState = new MapStateDescriptor<>(key, stringTypeInfo, riskResultTypeInfo);
            StateTtlConfig riskResultStateConfig = StateTtlConfig
                    .newBuilder(Time.minutes(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            risResultState.enableTimeToLive(riskResultStateConfig);

            MapState<String, GammaAggregate> aggregateStates = this.getRuntimeContext().getMapState(risResultState);

            GammaAggregate riskAccu;
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