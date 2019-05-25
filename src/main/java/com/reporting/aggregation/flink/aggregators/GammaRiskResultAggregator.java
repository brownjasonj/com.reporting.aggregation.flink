package com.reporting.aggregation.flink.aggregators;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Gamma;
import com.reporting.mocks.model.risks.aggregate.GammaAggregate;
import com.reporting.mocks.model.risks.aggregate.RiskAggregationException;

public class GammaRiskResultAggregator extends RiskResultAggregator<Gamma, GammaAggregate> {
    private static final long serialVersionUID = 1L;
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
    
}