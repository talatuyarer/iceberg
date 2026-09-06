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
package org.apache.iceberg.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.aws.s3.VendedCredentialsProvider;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.verify.VerificationTimes;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Verifies that a {@link SerializableTable} Java-serialization round trip preserves the {@link
 * S3FileIO} vended-credentials refresh wiring, by rebuilding a provider from only the surviving
 * properties and refreshing it against a live mock endpoint. See {@link
 * org.apache.iceberg.flink.sink.TestSinkCredentialRefresh} for the end-to-end proof with running
 * sinks.
 *
 * <p>Kryo is not covered: Flink ships operators via Java serialization, and Flink 2.x bundles Kryo
 * 5, incompatible with the Kryo 4 test helper. Kryo coverage of the refresh wiring lives in the aws
 * module ({@code TestS3FileIOCredentialRefresh#refreshedCredentialsAreKryoSerializable}).
 */
public class TestVendedCredentialsSerialization {

  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of("db", "tbl");

  @RegisterExtension
  private static final RESTServerExtension REST_SERVER_EXTENSION =
      new RESTServerExtension(
          ImmutableMap.of(
              RESTCatalogServer.REST_PORT,
              RESTServerExtension.FREE_PORT,
              CatalogProperties.CLIENT_POOL_SIZE,
              "1"));

  private static ClientAndServer mockServer;
  private static String credentialsEndpoint;

  private RESTCatalog catalog;

  @BeforeAll
  static void startMockServer() {
    mockServer = startClientAndServer();
    credentialsEndpoint = String.format("http://127.0.0.1:%d/v1/credentials", mockServer.getPort());
  }

  @AfterAll
  static void stopMockServer() {
    mockServer.stop();
  }

  @BeforeEach
  void createCatalogAndTable() {
    mockServer.reset();
    String uri =
        String.format(
            "http://localhost:%s/",
            REST_SERVER_EXTENSION.config().get(RESTCatalogServer.REST_PORT));
    Map<String, String> clientProps =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, uri)
            .put(CatalogProperties.CLIENT_POOL_SIZE, "1")
            .put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName())
            .put(AwsClientProperties.CLIENT_REGION, "us-east-1")
            .put(S3FileIOProperties.ACCESS_KEY_ID, "expiredGarbageKey")
            .put(S3FileIOProperties.SECRET_ACCESS_KEY, "expiredGarbageSecret")
            .put(S3FileIOProperties.SESSION_TOKEN, "expiredGarbageToken")
            .put(
                S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                Long.toString(Instant.now().minusSeconds(1).toEpochMilli()))
            .put(AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT, credentialsEndpoint)
            .build();
    this.catalog = new RESTCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize("vended", clientProps);
    if (!catalog.namespaceExists(Namespace.of("db"))) {
      catalog.createNamespace(Namespace.of("db"));
    }
    catalog.createTable(TABLE_IDENT, SimpleDataUtil.SCHEMA);
  }

  @AfterEach
  void dropTable() throws Exception {
    catalog.dropTable(TABLE_IDENT);
    catalog.close();
  }

  @Test
  void refreshWiringSurvivesJavaSerialization() throws Exception {
    // org.apache.iceberg.TestHelpers (api module), not org.apache.iceberg.flink.TestHelpers
    Table roundTripped =
        TestHelpers.roundTripSerialize(SerializableTable.copyOf(catalog.loadTable(TABLE_IDENT)));
    assertRefreshWorksFrom(roundTripped);
  }

  private void assertRefreshWorksFrom(Table roundTripped) {
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
                                    .prefix("s3")
                                    .config(
                                        ImmutableMap.of(
                                            S3FileIOProperties.ACCESS_KEY_ID,
                                            "refreshedKey",
                                            S3FileIOProperties.SECRET_ACCESS_KEY,
                                            "refreshedSecret",
                                            S3FileIOProperties.SESSION_TOKEN,
                                            "refreshedToken",
                                            S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                                            Long.toString(
                                                Instant.now()
                                                    .plus(1, ChronoUnit.HOURS)
                                                    .toEpochMilli())))
                                    .build())
                            .build())));

    assertThat(roundTripped.io()).isInstanceOf(S3FileIO.class);
    Map<String, String> ioProps = roundTripped.io().properties();
    assertThat(ioProps)
        .containsEntry(AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT, credentialsEndpoint)
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "expiredGarbageKey")
        .containsKey(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS)
        .containsKey(CatalogProperties.URI);

    AwsCredentialsProvider provider =
        new AwsClientProperties(ioProps)
            .credentialsProvider(
                "expiredGarbageKey", "expiredGarbageSecret", "expiredGarbageToken");
    assertThat(provider).isInstanceOf(VendedCredentialsProvider.class);
    try (VendedCredentialsProvider vended = (VendedCredentialsProvider) provider) {
      AwsCredentials refreshed = vended.resolveCredentials();
      assertThat(refreshed.accessKeyId()).isEqualTo("refreshedKey");
    }
    mockServer.verify(request().withPath("/v1/credentials"), VerificationTimes.atLeast(1));
  }
}
