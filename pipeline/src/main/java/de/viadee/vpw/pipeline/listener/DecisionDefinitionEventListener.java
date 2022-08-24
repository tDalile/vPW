package de.viadee.vpw.pipeline.listener;

import com.fasterxml.jackson.databind.JsonNode;
import de.viadee.camunda.kafka.event.DecisionDefinitionEvent;
import de.viadee.vpw.pipeline.PipelineApplication;
import de.viadee.vpw.pipeline.config.properties.ApplicationProperties;
import de.viadee.vpw.pipeline.service.json.JsonMapper;
import de.viadee.vpw.shared.config.elasticsearch.ElasticsearchProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Component
public class DecisionDefinitionEventListener {

    private final Logger logger = LoggerFactory.getLogger(DecisionDefinitionEventListener.class);

    private final JsonMapper jsonMapper;

    private final RestTemplate restTemplate;

    private final ApplicationProperties properties;

    @Autowired
    public DecisionDefinitionEventListener(JsonMapper jsonMapper, RestTemplate restTemplate,
                                           ApplicationProperties properties) {
        this.jsonMapper = jsonMapper;
        this.restTemplate = restTemplate;
        this.properties = properties;
    }

    @KafkaListener(topics = "${vpw.pipeline.kafka.topics.decision-definition}", clientIdPrefix = "${spring.kafka.consumer.client-id}-decision-definition")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        logger.trace("Received record: {}", record);
        DecisionDefinitionEvent event = jsonMapper.fromJson(record.value(), DecisionDefinitionEvent.class);
        this.importDecisionDefinition(event, acknowledgment);
    }

    @Retryable(recover = "shutdown", value = RestClientException.class,
            maxAttempts = 120, backoff = @Backoff(delay = 5000))
    public void importDecisionDefinition(DecisionDefinitionEvent event, Acknowledgment acknowledgment) {

        String decisionDefinitionId = event.getId();
        logger.info("Importing decision definition '{}'", decisionDefinitionId);
            HttpStatus status = postDecisionDefinition(event);
            if (status.is2xxSuccessful()) {
                logger.info("Import of decision definition '{}' finished successfully", decisionDefinitionId);
                acknowledgment.acknowledge();
            } else {
                logger.warn("Import of decision definition '{}' returned status {}", decisionDefinitionId, status);
            }
    }

    @Recover
    public void shutdown(RestClientException e) {
        logger.error("Shutdown Application caused by", e);
        PipelineApplication.exitApplication(PipelineApplication.getCtx());
    }

    private HttpStatus postDecisionDefinition(DecisionDefinitionEvent event) {
        return restTemplate.postForEntity(properties.getProcessDefinitionRestUrl(), event, JsonNode.class)
                .getStatusCode();
    }
}
