package se.irori.kroxylicious.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;

@Plugin(configType = OversizeFilterConfig.class)
//@Log4j2
public class OversizeProduceFilterFactory extends OversizeFilterFactory implements FilterFactory<OversizeFilterConfig, Object> {

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
                config.storageType(),
                format("Config typ missing, check yaml config. Valid types: %s",
                        stream(Type.values())
                                .map(Enum::name)
                                .collect(Collectors.joining(","))));

        this.config = config;
        return config;
    }

    //TODO filters is being recreated continously, is this correct?
    @Override
    public Filter createFilter(FilterFactoryContext context, Object initializationData) {
        return new OversizeProduceFilter(createStorageFromConfig(config));
    }

}


