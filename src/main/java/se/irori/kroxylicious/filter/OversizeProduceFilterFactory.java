package se.irori.kroxylicious.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import lombok.extern.log4j.Log4j2;
import se.irori.kroxylicious.filter.storage.TempFileOversizeStorage;

import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;

@Plugin(configType = OversizeFilterConfig.class)
@Log4j2
public class OversizeProduceFilterFactory implements FilterFactory<OversizeFilterConfig, Object> {

    public enum Type {
        LOCAL_TEMP_FILE
    }

    private OversizeFilterConfig config;

    @Override
    public OversizeFilterConfig initialize(
            FilterFactoryContext context,
            OversizeFilterConfig config)
            throws PluginConfigurationException {

        requireNonNull(config,
                "OversizeFilterConfig missing, check yaml config");

        requireNonNull(
                config.type(),
                format("Config typ missing, check yaml config. Valid types: %s",
                        stream(Type.values())
                                .map(Enum::name)
                                .collect(Collectors.joining(","))));

        this.config = config;
        return config;
    }

    @Override
    public Filter createFilter(
            FilterFactoryContext context,
            Object initializationData) {


        if (config.type() == null) {
            log.error("Type not configured");
            throw new OversizeFilterConfigException();
        }

        log.info("Type: {}", config.type());
        if (config.type() == Type.LOCAL_TEMP_FILE) {
            return new OversizeProduceFilter(new TempFileOversizeStorage());
        }

        log.error("Unsupported Type: {}", config.type());
        throw new OversizeFilterConfigException();

    }

}


