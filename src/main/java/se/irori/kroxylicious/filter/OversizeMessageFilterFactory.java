package se.irori.kroxylicious.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import lombok.extern.log4j.Log4j2;

import static java.util.Objects.requireNonNull;
import static se.irori.kroxylicious.filter.PersistorType.LOCAL_TEMP_FILE;

@Plugin(configType = OversizeMessageFilterConfig.class)
@Log4j2
public class OversizeMessageFilterFactory implements FilterFactory<OversizeMessageFilterConfig, Object> {

    private OversizeMessageFilterConfig config;

    @Override
    public OversizeMessageFilterConfig initialize(
            FilterFactoryContext context,
            OversizeMessageFilterConfig config) throws PluginConfigurationException {

        requireNonNull(config,
                "OversizeMessageFilterConfig missing, check yaml config");
        requireNonNull(config.persistorType(),
                "OversizeMessageFilterConfig.persistorType() missing, check yaml config");

        this.config = config;
        return config;
    }

    @Override
    public Filter createFilter(
            FilterFactoryContext context,
            Object initializationData) {


        if (config.persistorType() == null) {
            log.error("PersistorType not configured");
            throw new RuntimeException();
        }

        log.info("PersistorType: {}", config.persistorType());
        if (config.persistorType() == LOCAL_TEMP_FILE) {
            return new OversizeMessageFilter(new FileMessagePersistor());
        }

        log.error("Unsupported PersistorType: {}", config.persistorType());
        throw new RuntimeException();

    }

}


