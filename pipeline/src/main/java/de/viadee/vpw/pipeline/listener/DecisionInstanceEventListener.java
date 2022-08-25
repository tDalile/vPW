package de.viadee.vpw.pipeline.listener;

import de.viadee.vpw.pipeline.kafka.KafkaAcknowledgment;
import de.viadee.vpw.pipeline.model.DecisionInstanceEvent;
import de.viadee.vpw.pipeline.model.ProcessInstanceEvent;
import de.viadee.vpw.pipeline.service.elastic.ElasticsearchBulkIndexer;
import de.viadee.vpw.pipeline.service.elastic.ElasticsearchRequestBuilder;
import de.viadee.vpw.pipeline.service.json.JsonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class DecisionInstanceEventListener {

    private final Logger logger = LoggerFactory.getLogger(DecisionInstanceEventListener.class);

    private final ElasticsearchRequestBuilder requestBuilder;

    private final ElasticsearchBulkIndexer bulkIndexer;

    private final JsonMapper jsonMapper;

    @Autowired
    public DecisionInstanceEventListener(ElasticsearchRequestBuilder requestBuilder,
                                         ElasticsearchBulkIndexer bulkIndexer, JsonMapper jsonMapper) {
        this.requestBuilder = requestBuilder;
        this.bulkIndexer = bulkIndexer;
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(topics = "${vpw.pipeline.kafka.topics.decision-instance}", clientIdPrefix = "${spring.kafka.consumer.client-id}-decision-instance")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        logger.trace("Received record: {}", record);
        DecisionInstanceEvent event = jsonMapper.fromJson(record.value(), DecisionInstanceEvent.class);
        IndexRequest request = requestBuilder.buildIndexRequest(event.getId(), event,
                new KafkaAcknowledgment(record, acknowledgment));
        bulkIndexer.add(request);
    }

}
