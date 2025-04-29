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

Configure ports
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
```

### AWS_S3

``` 
filterDefinitions:
  - name: oversizeProduceFilter
    type: OversizeProduceFilterFactory
    config:
      storageType: AWS_S3
      properties:
        BASE_URL: ${BASE_URL}
        REGION: ${REGION}
        BUCKET_NAME: ${BUCKET_NAME}
        S3_ACCESS_KEY_KEY: ${S3_ACCESS_KEY_KEY}
        S3_SECRET_KEY: ${S3_SECRET_KEY}
  - name: oversizeConsumeFilter
    type: OversizeConsumeFilterFactory
    config:
      storageType: AWS_S3
      properties:
        BASE_URL: ${BASE_URL}
        REGION: ${REGION}
        BUCKET_NAME: ${BUCKET_NAME}
        S3_ACCESS_KEY_KEY: ${S3_ACCESS_KEY_KEY}
        S3_SECRET_KEY: ${S3_SECRET_KEY}
defaultFilters:
  - oversizeProduceFilter
  - oversizeConsumeFilter
```

### LOCAL_TEMP_FILE

``` 
filterDefinitions:
- name: oversizeProduceFilter
  type: OversizeProduceFilterFactory
  config:
  storageType: LOCAL_TEMP_FILE
- name: oversizeConsumeFilter
  type: OversizeConsumeFilterFactory
  config:
  storageType: LOCAL_TEMP_FILE
  defaultFilters:
- oversizeProduceFilter
- oversizeConsumeFilter
```