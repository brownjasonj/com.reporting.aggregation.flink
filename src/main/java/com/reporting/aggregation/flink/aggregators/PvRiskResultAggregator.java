package com.reporting.aggregation.flink.aggregators;

import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Pv;
import com.reporting.mocks.model.risks.aggregate.PvAggregate;
import com.reporting.mocks.model.risks.aggregate.RiskAggregationException;

import java.util.logging.Level;
import java.util.logging.Logger;

public class PvRiskResultAggregator extends RiskResultAggregator<Pv, PvAggregate> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(PvRiskResultAggregator.class.getName());


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
}