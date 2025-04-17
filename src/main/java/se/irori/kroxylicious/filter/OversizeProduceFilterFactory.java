package se.irori.kroxylicious.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import lombok.extern.log4j.Log4j2;
import se.irori.kroxylicious.filter.persist.TempFilePersistor;

import static java.util.Objects.requireNonNull;
import static se.irori.kroxylicious.filter.persist.OversizePersistor.Type.LOCAL_TEMP_FILE;

@Plugin(configType = OversizeFilterConfig.class)
@Log4j2
public class OversizeProduceFilterFactory implements FilterFactory<OversizeFilterConfig, Object> {

    private OversizeFilterConfig config;

    @Override
    public OversizeFilterConfig initialize(
            FilterFactoryContext context,
            OversizeFilterConfig config) throws PluginConfigurationException {

        requireNonNull(config,
                "OversizeFilterConfig missing, check yaml config");
        requireNonNull(config.persistorType(),
                "OversizeFilterConfig.persistorType() missing, check yaml config");

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
            return new OversizeProduceFilter(new TempFilePersistor());
        }

        log.error("Unsupported PersistorType: {}", config.persistorType());
        throw new RuntimeException();

    }

}


