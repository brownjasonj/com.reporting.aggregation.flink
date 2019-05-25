package com.reporting.aggregation.flink.filters;

import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Risk;
import com.reporting.mocks.model.risks.RiskType;
import org.apache.flink.api.common.functions.FilterFunction;

public class RiskResultFilterByRiskType<R extends Risk> implements FilterFunction<RiskResult<R>> {
    private static final long serialVersionUID = 5557311593784189107L;

    protected RiskType riskType;

    public RiskResultFilterByRiskType(Class<R> clazz) throws IllegalAccessException, InstantiationException {
        this.riskType = clazz.newInstance().getRiskType();
    }

    @Override
    public boolean filter(RiskResult<R> value) throws Exception {
        return value.getRisk().getRiskType() == this.riskType;
    }
}
