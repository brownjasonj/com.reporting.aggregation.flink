package com.reporting.aggregation.flink.aggregators;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Delta;
import com.reporting.mocks.model.risks.aggregate.DeltaAggregate;
import com.reporting.mocks.model.risks.aggregate.RiskAggregationException;

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

    
    
}