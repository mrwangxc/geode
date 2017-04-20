/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

@Category(DistributedTest.class)
public class ShowMissingDiskStoresDUnitTest {

  // private static final String DISK_STORE = "diskStore";
  private static final String DISK_STORE_DIR = "myDiskStores";
  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();

  @Before
  public void before() throws Exception {
    locator = lsRule.startLocatorVM(0);
    gfshConnector.connect(locator);
    assertThat(gfshConnector.isConnected()).isTrue();

    // start a server so that we can execute data commands that requires at least a server running
    Properties localProps = new Properties();
    localProps.setProperty(GROUPS, "Group1");
    lsRule.startServerVM(1, localProps, locator.getPort());

    localProps = new Properties();
    localProps.setProperty(GROUPS, "Group2");
    lsRule.startServerVM(2, localProps, locator.getPort());
    server1 = (MemberVM) lsRule.getMember(1);
    server2 = (MemberVM) lsRule.getMember(2);
  }

  @Test
  public void missingDiskStores_gfshDoesntHang() throws Exception {

    final String testNameString = testName.getMethodName();
    final String testRegionName = testNameString + "Region";
    CommandStringBuilder csb;
    Path diskStoreDir = Paths.get(server1.getWorkingDir().getAbsolutePath(), DISK_STORE_DIR);
    csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE)
        .addOption(CliStrings.CREATE_DISK_STORE__NAME, testNameString)
        .addOption(CliStrings.CREATE_DISK_STORE__GROUP, "Group1")
        .addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, diskStoreDir.toString());
    gfshConnector.executeAndVerifyCommand(csb.getCommandString());
    diskStoreDir = Paths.get(server2.getWorkingDir().getAbsolutePath(), DISK_STORE_DIR);
    csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE)
        .addOption(CliStrings.CREATE_DISK_STORE__NAME, testNameString)
        .addOption(CliStrings.CREATE_DISK_STORE__GROUP, "Group2")
        .addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, diskStoreDir.toString());
    gfshConnector.executeAndVerifyCommand(csb.getCommandString());
    // TODO: Fix the disk-store-dir argument ...
    csb = new CommandStringBuilder(CliStrings.CREATE_REGION)
        .addOption(CliStrings.CREATE_REGION__REGION, testRegionName)
        .addOption(CliStrings.CREATE_REGION__DISKSTORE, testNameString)
        .addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT,
            RegionShortcut.PARTITION_PERSISTENT.toString());
    gfshConnector.executeAndVerifyCommand(csb.getCommandString());

    // Add data to the region
    csb = new CommandStringBuilder(CliStrings.PUT).addOption(CliStrings.PUT__KEY, "A")
        .addOption(CliStrings.PUT__VALUE, "B")
        .addOption(CliStrings.PUT__REGIONNAME, testRegionName);
    gfshConnector.executeAndVerifyCommand(csb.getCommandString());

    // stop the servers in sequence - on restart there should be a missing disk-store
    server1.invoke(() -> {
      LocatorServerStartupRule.serverStarter.stopMember();
    });
    server2.invoke(() -> {
      LocatorServerStartupRule.serverStarter.stopMember();
    });

    String workingDirString = server1.getWorkingDir().getAbsolutePath().toString();
    int locatorPort = locator.getPort();
//    server1.invoke(() -> {
//     Properties localProps = new Properties();
//
//     localProps.setProperty(ConfigurationProperties.NAME, "server-1");
//     localProps.setProperty(GROUPS, "Group1");
//     ServerStarterRule serverStarter = new ServerStarterRule(new File(workingDirString));
//     serverStarter.withProperties(localProps).withConnectionToLocator(locatorPort).withAutoStart();
//     serverStarter.before();
//   });
    csb = new CommandStringBuilder(CliStrings.EXPORT_SHARED_CONFIG)
        .addOption(CliStrings.EXPORT_SHARED_CONFIG__FILE, CliStrings.EXPORT_SHARED_CONFIG__FILE__NAME);
    gfshConnector.executeAndVerifyCommand(csb.getCommandString());
    csb = new CommandStringBuilder(CliStrings.START_SERVER)
        .addOption(CliStrings.START_SERVER__NAME, server1.getName())
        .addOption(CliStrings.START_SERVER__USE_CLUSTER_CONFIGURATION)
        .addOption(CliStrings.START_SERVER__DIR, Paths.get(server1.getWorkingDir().getAbsolutePath()).toString());
    gfshConnector.executeAndVerifyCommand(csb.getCommandString());

    csb = new CommandStringBuilder(CliStrings.STOP_SERVER).addOption(CliStrings.STOP_SERVER__MEMBER,
        server2.getName());
    gfshConnector.executeAndVerifyCommand(csb.getCommandString());


  }


}
