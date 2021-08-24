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

import com.dremio.exec.store.jdbc.*;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import org.apache.log4j.Logger;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;

/**
 * Configuration for Netezza sources.
 */
@SourceType(value = "NETEZZARP", label = "Netezza", uiConfig = "netezza-layout.json", externalQuerySupported = true)
public class NetezzaConf extends AbstractArpConf<NetezzaConf> {
  private static final String ARP_FILENAME = "arp/implementation/netezza-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
  private static final String DRIVER = "org.netezza.Driver";
  private static Logger logger = Logger.getLogger(NetezzaConf.class);


  @Tag(1)
  @DisplayMetadata(label = "JDBC String (example: jdbc:netezza://localhost:5480/system")
  public String jdbcString;


  @Tag(2)
  @DisplayMetadata(label = "username")
  public String username;


  @Tag(3)
  @Secret
  @DisplayMetadata(label = "password")
  public String password;

  @Tag(4)
  @DisplayMetadata(label = "Record fetch size")
  @NotMetadataImpacting
  public int fetchSize = 200;

  @Tag(5)
  @NotMetadataImpacting
  @JsonIgnore
  public boolean enableExternalQuery = false;

  @Tag(6)
  @DisplayMetadata(label = "Maximum idle connections")
  @NotMetadataImpacting
  public int maxIdleConns = 8;

  @Tag(7)
  @DisplayMetadata(label = "Connection idle time (s)")
  @NotMetadataImpacting
  public int idleTimeSec = 60;

  @VisibleForTesting
  public String toJdbcConnectionString() {
    final String jdbcString = checkNotNull(this.jdbcString, "Missing database.");
    checkNotNull(this.username, "Missing username.");
    checkNotNull(this.password, "Missing password.");
    return String.format("%s", jdbcString);
  }

  @Override
  @VisibleForTesting
  public JdbcPluginConfig buildPluginConfig(
          JdbcPluginConfig.Builder configBuilder,
          CredentialsService credentialsService,
          OptionManager optionManager
  ) {
    return configBuilder.withDialect(getDialect())
            .withDialect(getDialect())
            .withFetchSize(fetchSize)
            .withDatasourceFactory(this::newDataSource)
            .clearHiddenSchemas()
            .addHiddenSchema("SYSTEM")
            .build();
  }


  private CloseableDataSource newDataSource() {
    return DataSources.newGenericConnectionPoolDataSource(DRIVER,
            toJdbcConnectionString(), username, password, null,
            DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE, maxIdleConns, idleTimeSec);
  }

  @Override
  public ArpDialect getDialect() {
    return ARP_DIALECT;
  }

  @VisibleForTesting
  public static ArpDialect getDialectSingleton() {
    return ARP_DIALECT;
  }
}
