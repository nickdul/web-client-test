# HTTP Client Test
Performance tests for various HTTP clients.

## Available HTTP Clients

|Name       | Description
|---        |---
|OKHTTP     |[OkHttp 4.x](https://square.github.io/okhttp/changelogs/changelog_4x/)
|JAVA       |Standard Java HTTP Client
|AHC        |[AsyncHttpClient 2.12](https://github.com/AsyncHttpClient/async-http-client/tree/2.12.4-SNAPSHOT)
|APACHE     |[Apache HTTP Client 5.x](https://hc.apache.org/httpcomponents-client-5.3.x/index.html#)
|NETTY      |[Netty - Sigle Channel](https://netty.io/index.html)

## Run HTTP Client Test

```bash

TEST_APP_HOME=~/web-client-test

JAVA_OPTS=-XX:MaxRAMPercentage=50 -XX:+AlwaysActAsServerClassMachine --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true

API_KEY="<API_KEY>"
API_SECRET="<API_SECRET>"

TEST_CLASS=deltix.web.client.WebClientTest
WAIT_PERIOD=200
BATCH_COUNT=20
REQUEST_IN_BATCH=5
NATIVE_IO=false

java $JAVA_OPTS -cp $TEST_APP_HOME/* $TEST_CLASS $API_KEY $API_SECRET NETTY,OKHTTP,JAVA,AHC,APACHE $WAIT_PERIOD $BATCH_COUNT $REQUEST_IN_BATCH $NATIVE_IO

```
