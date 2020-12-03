/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.jdbc.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import org.hibernate.validator.constraints.NotBlank;
import org.apache.log4j.Logger;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin.Config;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.google.common.annotations.VisibleForTesting;

import io.protostuff.Tag;

/**
 * Configuration for SQLite sources.
 */
@SourceType(value = "NETEZZA", label = "Netezza", uiConfig = "netezza-layout.json")
public class NetezzaConf extends AbstractArpConf<NetezzaConf> {
  private static final String ARP_FILENAME = "arp/implementation/netezza-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
  private static final String DRIVER = "org.netezza.Driver";
  private static Logger logger = Logger.getLogger(NetezzaConf.class);

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "JDBC String (example: jdbc:netezza://localhost:5480/system")
  public String jdbcString;

  @NotBlank
  @Tag(2)
  @DisplayMetadata(label = "username")
  public String username;

  @NotBlank
  @Tag(3)
  @DisplayMetadata(label = "password")
  public String password;

  @Tag(4)
  @DisplayMetadata(label = "Record fetch size")
  @NotMetadataImpacting
  public int fetchSize = 200;

  @Tag(5)
  @NotMetadataImpacting
  @DisplayMetadata(label = ENABLE_EXTERNAL_QUERY_LABEL)
  public boolean enableExternalQuery = false;

  @VisibleForTesting
  public String toJdbcConnectionString() {
    final String jdbcString = checkNotNull(this.jdbcString, "Missing database.");
    checkNotNull(this.username, "Missing username.");
    checkNotNull(this.password, "Missing password.");
    logger.info(String.format("%s", jdbcString));
    return String.format("%s", jdbcString);
  }

  @Override
  @VisibleForTesting
  public Config toPluginConfig(CredentialsService credentialsService, OptionManager optionManager) {
    logger.info("in toPluginConfig");
    return JdbcStoragePlugin.Config.newBuilder()
        .withDialect(getDialect())
        .withFetchSize(fetchSize)
        .withDatasourceFactory(this::newDataSource)
        .clearHiddenSchemas()
        .addHiddenSchema("SYSTEM")
        .withAllowExternalQuery(enableExternalQuery)
        .build();
  }

  private CloseableDataSource newDataSource() {
    logger.info("in CloseableDataSource");
    return DataSources.newGenericConnectionPoolDataSource(DRIVER,
      toJdbcConnectionString(), username, password, null, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
  }

  @Override
  public ArpDialect getDialect() {
    logger.info("in ArpDialect");
    return ARP_DIALECT;
  }

  @VisibleForTesting
  public static ArpDialect getDialectSingleton() {
    logger.info("in ArpDialect");
    return ARP_DIALECT;
  }
}
