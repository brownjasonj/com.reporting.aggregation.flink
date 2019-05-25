package com.reporting.aggregation.flink.selectors;

import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Risk;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.UUID;

public class RiskResultMarketIdKeySelector<R extends Risk> implements KeySelector<RiskResult<R>, UUID> {

    private static final long serialVersionUID = -10049342818881476L;

    @Override
    public UUID getKey(RiskResult<R> value) throws Exception {
        return value.getRisk().getMarketEnvId().getId();
    }
}
