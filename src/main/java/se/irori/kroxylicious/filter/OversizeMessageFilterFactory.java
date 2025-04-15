package se.irori.kroxylicious.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

@Plugin(configType = Object.class)
public class OversizeMessageFilterFactory implements FilterFactory<Object, Object> {

    @Override
    public Object initialize(FilterFactoryContext context, Object config) throws PluginConfigurationException {
        return null;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Object initializationData) {
        return new OversizeMessageFilter();
    }

}


