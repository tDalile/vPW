package de.viadee.vpw.pipeline.model;

import java.util.Date;

public class DecisionInstanceEvent extends de.viadee.camunda.kafka.event.DecisionInstanceEvent
        implements ElasticsearchEntity {

    static final String TYPE = "decision";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Date getTimestamp() { return getEvaluationTime(); }

}
