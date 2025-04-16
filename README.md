# kroxylicious_filters

Tested on kroxylicious-app-0.11.0, not guaranteed to work in other versions.

To use:

- download [kroxylicious-app-0.11.0](https://github.com/kroxylicious/kroxylicious/releases)
- expand the zip/tar 
- export KROXYLICIOUS_HOME=/folder-you-selected/kroxylicious-app-0.11
- build this project and copy the resulting jar to the kroxylicious "libs" folder
- create proxy-config.yaml, see below
- Start server: /bin/sh \${KROXYLICIOUS_HOME}/bin/kroxylicious-start.sh --config=${KROXYLICIOUS_HOME}/config/proxy-config.yaml


## kroxylicious config file

```
#proxy-config.yaml

management:
  endpoints:
    prometheus: {}
virtualClusters:
  - name: demo
    targetCluster:
      bootstrapServers: localhost:9092
    gateways:
    - name: mygateway
      portIdentifiesNode:
        bootstrapAddress: localhost:9192
    logNetwork: false
    logFrames: false
filterDefinitions:
  - name: oversizeMessageFilter
    type: OversizeMessageFilterFactory
    config:
      persistorType: LOCAL_TEMP_FILE
defaultFilters:
  - oversizeMessageFilter
```