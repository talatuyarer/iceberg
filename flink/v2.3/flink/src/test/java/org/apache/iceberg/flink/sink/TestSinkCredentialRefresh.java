/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogServer;
import org.apache.iceberg.rest.RESTServerExtension;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponseParser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockserver.integration.ClientAndServer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * Verifies that {@link FlinkSink} and {@link IcebergSink} jobs succeed when the only valid S3
 * credentials are served by the vended-credentials refresh endpoint: all statically configured
 * credentials are expired garbage, so writes require each subtask's deserialized {@code S3FileIO}
 * to refresh. {@link #sinkFailsWithoutRefreshEndpoint()} is the negative control.
 *
 * <p>Requires Docker for the MinIO container; skipped when Docker is not available.
 */
public class TestSinkCredentialRefresh {

  private static final String BUCKET = "flink-refresh-test";

  private static final boolean DOCKER_AVAILABLE = isDockerAvailable();

  private static final MinIOContainer MINIO =
      new MinIOContainer(DockerImageName.parse("minio/minio:latest"))
          .withEnv("MINIO_DOMAIN", "localhost");

  static {
    // must be running before REST_SERVER_EXTENSION below captures the MinIO URL
    if (DOCKER_AVAILABLE) {
      MINIO.start();
    }
  }

  @RegisterExtension
  private static final RESTServerExtension REST_SERVER_EXTENSION =
      new RESTServerExtension(restServerConfig());

  @RegisterExtension
  public static MiniClusterExtension miniCluster =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  private static final TypeInformation<Row> ROW_TYPE_INFO =
      new RowTypeInfo(
          SimpleDataUtil.FLINK_SCHEMA.getColumnDataTypes().stream()
              .map(ExternalTypeInfo::of)
              .toArray(TypeInformation[]::new));

  private static final DataFormatConverters.RowConverter CONVERTER =
      new DataFormatConverters.RowConverter(
          SimpleDataUtil.FLINK_SCHEMA.getColumnDataTypes().toArray(DataType[]::new));

  private static ClientAndServer mockServer;

  private RESTCatalog clientCatalog;
  private Map<String, String> clientProps;
  private TableIdentifier tableIdent;

  private static S3Client rootS3;

  private static boolean isDockerAvailable() {
    try {
      return DockerClientFactory.instance().isDockerAvailable();
    } catch (Exception e) {
      return false;
    }
  }

  private static Map<String, String> restServerConfig() {
    if (!DOCKER_AVAILABLE) {
      // MINIO.getS3URL() would throw on an unstarted container; the tests get skipped anyway
      return ImmutableMap.of(RESTCatalogServer.REST_PORT, RESTServerExtension.FREE_PORT);
    }

    return ImmutableMap.<String, String>builder()
        .put(RESTCatalogServer.REST_PORT, RESTServerExtension.FREE_PORT)
        .put(CatalogProperties.CLIENT_POOL_SIZE, "1")
        .put(CatalogProperties.WAREHOUSE_LOCATION, "s3://" + BUCKET + "/warehouse")
        .put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName())
        .put(S3FileIOProperties.ENDPOINT, MINIO.getS3URL())
        .put(S3FileIOProperties.PATH_STYLE_ACCESS, "true")
        .put(S3FileIOProperties.ACCESS_KEY_ID, MINIO.getUserName())
        .put(S3FileIOProperties.SECRET_ACCESS_KEY, MINIO.getPassword())
        .put(AwsClientProperties.CLIENT_REGION, "us-east-1")
        .build();
  }

  @BeforeAll
  static void startClientsAndServers() {
    assumeThat(DOCKER_AVAILABLE).as("Docker is not available").isTrue();

    mockServer = ClientAndServer.startClientAndServer();

    rootS3 =
        S3Client.builder()
            .endpointOverride(URI.create(MINIO.getS3URL()))
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(MINIO.getUserName(), MINIO.getPassword())))
            .build();
    rootS3.createBucket(b -> b.bucket(BUCKET));
  }

  @AfterAll
  static void stopClientsAndServers() {
    if (rootS3 != null) {
      rootS3.close();
    }
    if (mockServer != null) {
      mockServer.stop();
    }
    MINIO.stop();
  }

  @BeforeEach
  void setupCredentialVendingAndTable(TestInfo testInfo) {
    assumeThat(DOCKER_AVAILABLE).as("Docker is not available").isTrue();

    mockServer.reset();

    // the refresh endpoint is the only source of credentials that MinIO accepts
    Credentials stsCreds = assumeRoleCredentials();
    mockServer
        .when(request().withPath("/v1/credentials"))
        .respond(
            response()
                .withStatusCode(200)
                .withBody(
                    LoadCredentialsResponseParser.toJson(
                        ImmutableLoadCredentialsResponse.builder()
                            .addCredentials(
                                ImmutableCredential.builder()
                                    .prefix("s3://" + BUCKET)
                                    .config(
                                        ImmutableMap.of(
                                            S3FileIOProperties.ACCESS_KEY_ID,
                                            stsCreds.accessKeyId(),
                                            S3FileIOProperties.SECRET_ACCESS_KEY,
                                            stsCreds.secretAccessKey(),
                                            S3FileIOProperties.SESSION_TOKEN,
                                            stsCreds.sessionToken(),
                                            S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                                            Long.toString(stsCreds.expiration().toEpochMilli())))
                                    .build())
                            .build())));

    String uri =
        String.format(
            "http://localhost:%s/",
            REST_SERVER_EXTENSION.config().get(RESTCatalogServer.REST_PORT));
    this.clientProps =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, uri)
            .put(CatalogProperties.CLIENT_POOL_SIZE, "1")
            .put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName())
            .put(AwsClientProperties.CLIENT_REGION, "us-east-1")
            .put(S3FileIOProperties.ENDPOINT, MINIO.getS3URL())
            .put(S3FileIOProperties.PATH_STYLE_ACCESS, "true")
            .put(S3FileIOProperties.ACCESS_KEY_ID, "expiredGarbageKey")
            .put(S3FileIOProperties.SECRET_ACCESS_KEY, "expiredGarbageSecret")
            .put(S3FileIOProperties.SESSION_TOKEN, "expiredGarbageToken")
            .put(
                S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                Long.toString(Instant.now().minusSeconds(1).toEpochMilli()))
            .put(
                AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT,
                String.format("http://127.0.0.1:%d/v1/credentials", mockServer.getPort()))
            .build();

    this.clientCatalog = new RESTCatalog();
    clientCatalog.setConf(new Configuration());
    clientCatalog.initialize("vended", clientProps);
    if (!clientCatalog.namespaceExists(Namespace.of("db"))) {
      clientCatalog.createNamespace(Namespace.of("db"));
    }
    this.tableIdent =
        TableIdentifier.of("db", testInfo.getTestMethod().orElseThrow().getName().toLowerCase());
    Table created = clientCatalog.createTable(tableIdent, SimpleDataUtil.SCHEMA);
    // fail fast if the REST server ignored the s3:// warehouse config
    assertThat(created.location()).startsWith("s3://" + BUCKET);
  }

  @AfterEach
  void cleanup() throws IOException {
    if (clientCatalog != null) {
      clientCatalog.dropTable(tableIdent, false /* keep data; bucket is throwaway */);
      clientCatalog.close();
    }
  }

  static Credentials assumeRoleCredentials() {
    try (StsClient sts =
        StsClient.builder()
            .endpointOverride(URI.create(MINIO.getS3URL()))
            .region(Region.US_EAST_1)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(MINIO.getUserName(), MINIO.getPassword())))
            .build()) {
      return sts.assumeRole(
              r ->
                  r.roleArn("arn:aws:iam::000000000000:role/anything")
                      .roleSessionName("flink-refresh-test")
                      .durationSeconds(900))
          .credentials();
    }
  }

  @Test
  void minioStsVendsUsableSessionCredentials() {
    Credentials stsCreds = assumeRoleCredentials();
    try (S3Client sessionS3 =
        S3Client.builder()
            .endpointOverride(URI.create(MINIO.getS3URL()))
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(
                        stsCreds.accessKeyId(),
                        stsCreds.secretAccessKey(),
                        stsCreds.sessionToken())))
            .build()) {
      sessionS3.putObject(b -> b.bucket(BUCKET).key("sts-probe"), RequestBody.fromString("ok"));
      assertThat(sessionS3.getObjectAsBytes(b -> b.bucket(BUCKET).key("sts-probe")).asUtf8String())
          .isEqualTo("ok");
    }
  }

  @Test
  void flinkSinkWritesWithRefreshedCredentialsOnly() throws Exception {
    runJobAndVerify(false);
  }

  @Test
  void icebergSinkWritesWithRefreshedCredentialsOnly() throws Exception {
    runJobAndVerify(true);
  }

  /**
   * Negative control: same expired credentials without the refresh endpoint, so the job must fail
   * with an S3 auth error. Proves the positive tests cannot pass vacuously, and trips if any other
   * credential path (e.g. a loadTable-based refresh) ever rescues the write. One sink suffices
   * since the failure is in the shared {@code S3FileIO} setup.
   */
  @Test
  void sinkFailsWithoutRefreshEndpoint() throws Exception {
    Map<String, String> propsWithoutRefresh =
        Maps.filterKeys(
            clientProps, key -> !AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT.equals(key));

    try (RESTCatalog noRefreshCatalog = new RESTCatalog()) {
      noRefreshCatalog.setConf(new Configuration());
      noRefreshCatalog.initialize("no-refresh", propsWithoutRefresh);

      Table table = noRefreshCatalog.loadTable(tableIdent);
      TableLoader tableLoader =
          TableLoader.fromCatalog(
              CatalogLoader.rest("no-refresh", new Configuration(), propsWithoutRefresh),
              tableIdent);

      // disable restarts so the expected failure surfaces instead of retrying forever
      org.apache.flink.configuration.Configuration noRestartConfig =
          new org.apache.flink.configuration.Configuration(
              MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
      noRestartConfig.set(
          RestartStrategyOptions.RESTART_STRATEGY,
          RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY.getMainValue());
      StreamExecutionEnvironment env =
          StreamExecutionEnvironment.getExecutionEnvironment(noRestartConfig)
              .enableCheckpointing(100);
      env.setParallelism(2);
      DataStream<Row> stream = boundedRowSource(env, testRows());

      FlinkSink.forRow(stream, SimpleDataUtil.FLINK_SCHEMA)
          .table(table)
          .tableLoader(tableLoader)
          .writeParallelism(2)
          .append();

      int refreshCallsBeforeJob =
          mockServer.retrieveRecordedRequests(request().withPath("/v1/credentials")).length;

      assertThatThrownBy(() -> env.execute("credential-refresh-no-endpoint"))
          .satisfies(
              thrown -> {
                boolean isS3AuthFailure =
                    ExceptionUtils.findThrowable(thrown, S3Exception.class).isPresent();
                if (!isS3AuthFailure) {
                  String chain = ExceptionUtils.stringifyException(thrown);
                  isS3AuthFailure =
                      ImmutableList.of(
                              "InvalidAccessKeyId",
                              "SignatureDoesNotMatch",
                              "InvalidTokenId",
                              "InvalidToken",
                              "Forbidden",
                              "403")
                          .stream()
                          .anyMatch(chain::contains);
                }
                assertThat(isS3AuthFailure)
                    .as("expected an S3 auth failure somewhere in the exception chain: %s", thrown)
                    .isTrue();
              });

      assertThat(mockServer.retrieveRecordedRequests(request().withPath("/v1/credentials")).length)
          .isEqualTo(refreshCallsBeforeJob);
    }
  }

  private static StreamExecutionEnvironment newBoundedStreamEnv() {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100);
    env.setParallelism(2);
    return env;
  }

  private static List<Row> testRows() {
    return Lists.newArrayList(Row.of(1, "aaa"), Row.of(2, "bbb"), Row.of(3, "ccc"));
  }

  private static DataStream<Row> boundedRowSource(StreamExecutionEnvironment env, List<Row> rows) {
    return env.addSource(new BoundedTestSource<>(ImmutableList.of(rows)), ROW_TYPE_INFO);
  }

  private void runJobAndVerify(boolean useV2Sink) throws Exception {
    Table table = clientCatalog.loadTable(tableIdent);
    TableLoader tableLoader =
        TableLoader.fromCatalog(
            CatalogLoader.rest("vended", new Configuration(), clientProps), tableIdent);

    StreamExecutionEnvironment env = newBoundedStreamEnv();
    List<Row> rows = testRows();
    DataStream<Row> stream = boundedRowSource(env, rows);

    if (useV2Sink) {
      IcebergSink.forRow(stream, SimpleDataUtil.FLINK_SCHEMA)
          .table(table)
          .tableLoader(tableLoader)
          .writeParallelism(2)
          .append();
    } else {
      FlinkSink.forRow(stream, SimpleDataUtil.FLINK_SCHEMA)
          .table(table)
          .tableLoader(tableLoader)
          .writeParallelism(2)
          .append();
    }

    int refreshCallsBeforeJob =
        mockServer.retrieveRecordedRequests(request().withPath("/v1/credentials")).length;

    env.execute("credential-refresh-" + (useV2Sink ? "v2" : "v1"));

    assertThat(mockServer.retrieveRecordedRequests(request().withPath("/v1/credentials")).length)
        .isGreaterThan(refreshCallsBeforeJob);

    List<RowData> expected = rows.stream().map(CONVERTER::toInternal).collect(Collectors.toList());
    SimpleDataUtil.assertTableRows(clientCatalog.loadTable(tableIdent), expected);
  }
}
