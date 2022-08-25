package de.viadee.vpw.pipeline.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = ApplicationProperties.PREFIX)
public class ApplicationProperties {

    static final String PREFIX = "vpw.pipeline";

    private String processDefinitionRestUrl;
    private String decisionDefinitionRestUrl;

    private String[] indexTemplates;

    public String getProcessDefinitionRestUrl() {
        return processDefinitionRestUrl;
    }

    public void setProcessDefinitionRestUrl(String processDefinitionRestUrl) {
        this.processDefinitionRestUrl = processDefinitionRestUrl;
    }

    public String getDecisionDefinitionRestUrl() {
        return decisionDefinitionRestUrl;
    }

    public void setDecisionDefinitionRestUrl(String decisionDefinitionRestUrl) {
        this.decisionDefinitionRestUrl = decisionDefinitionRestUrl;
    }

    public String[] getIndexTemplates() {
        return indexTemplates;
    }

    public void setIndexTemplates(String[] indexTemplates) {
        this.indexTemplates = indexTemplates;
    }
}
