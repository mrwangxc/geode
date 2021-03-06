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
 *
 */

package org.apache.geode.test.dunit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.dunit.Host.getHost;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;


/**
 * A rule to help you start locators and servers inside of a
 * <a href="https://cwiki.apache.org/confluence/display/GEODE/Distributed-Unit-Tests">DUnit
 * test</a>. This rule will start Servers and Locators inside of the four remote {@link VM}s created
 * by the DUnit framework.
 */
public class LocatorServerStartupRule extends ExternalResource implements Serializable {

  /**
   * This is only available in each Locator/Server VM, not in the controller (test) VM.
   */
  public static ServerStarterRule serverStarter;

  /**
   * This is only available in each Locator/Server VM, not in the controller (test) VM.
   */
  public static LocatorStarterRule locatorStarter;

  private DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  private TemporaryFolder temporaryFolder = new SerializableTemporaryFolder();
  private MemberVM[] members;
  private final boolean bounceVms;

  public LocatorServerStartupRule() {
    this(false);
  }

  /**
   * If your DUnit tests fail due to insufficient cleanup, try setting bounceVms=true.
   */
  public LocatorServerStartupRule(boolean bounceVms) {
    DUnitLauncher.launchIfNeeded();
    this.bounceVms = bounceVms;
  }

  @Override
  protected void before() throws Throwable {
    restoreSystemProperties.before();
    temporaryFolder.create();
    Invoke.invokeInEveryVM("Stop each VM", this::cleanupVm);
    if (bounceVms) {
      getHost(0).getAllVMs().forEach(VM::bounce);
    }
    members = new MemberVM[4];
  }

  @Override
  protected void after() {
    DUnitLauncher.closeAndCheckForSuspects();
    Invoke.invokeInEveryVM("Stop each VM", this::cleanupVm);
    restoreSystemProperties.after();
    temporaryFolder.delete();
  }

  public MemberVM startLocatorVM(int index) throws Exception {
    return startLocatorVM(index, new Properties());
  }

  /**
   * Starts a locator instance with the given configuration properties inside
   * {@code getHost(0).getVM(index)}.
   *
   * @return VM locator vm
   */
  public MemberVM<Locator> startLocatorVM(int index, Properties properties) throws Exception {
    String name = "locator-" + index;
    properties.setProperty(NAME, name);
    File workingDir = createWorkingDirForMember(name);
    VM locatorVM = getHost(0).getVM(index);
    Locator locator = locatorVM.invoke(() -> {
      locatorStarter = new LocatorStarterRule(workingDir);
      locatorStarter.withProperties(properties).withAutoStart();
      locatorStarter.before();
      return locatorStarter;
    });
    members[index] = new MemberVM(locator, locatorVM);
    return members[index];
  }


  public MemberVM startServerVM(int index) throws IOException {
    return startServerVM(index, new Properties(), -1);
  }

  public MemberVM startServerVM(int index, int locatorPort) throws IOException {
    return startServerVM(index, new Properties(), locatorPort);
  }

  public MemberVM startServerVM(int index, Properties properties) throws IOException {
    return startServerVM(index, properties, -1);
  }

  public MemberVM startServerAsJmxManager(int index) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(JMX_MANAGER_PORT, AvailablePortHelper.getRandomAvailableTCPPort() + "");
    return startServerVM(index, properties, -1);
  }

  public MemberVM startServerAsEmbededLocator(int index) throws IOException {
    String name = "server-" + index;
    File workingDir = createWorkingDirForMember(name);
    VM serverVM = getHost(0).getVM(index);
    Server server = serverVM.invoke(() -> {
      serverStarter = new ServerStarterRule(workingDir);
      serverStarter.withEmbeddedLocator().withName(name).withJMXManager().withAutoStart();
      serverStarter.before();
      return serverStarter;
    });
    members[index] = new MemberVM(server, serverVM);
    return members[index];
  }

  public void stopMember(int index) {
    MemberVM member = members[index];
    member.invoke(this::cleanupVm);
  }

  /**
   * Starts a cache server with given properties
   */
  public MemberVM startServerVM(int index, Properties properties, int locatorPort)
      throws IOException {
    String name = "server-" + index;
    properties.setProperty(NAME, name);

    File workingDir = createWorkingDirForMember(name);
    VM serverVM = getHost(0).getVM(index);
    Server server = serverVM.invoke(() -> {
      serverStarter = new ServerStarterRule(workingDir);
      serverStarter.withProperties(properties).withConnectionToLocator(locatorPort).withAutoStart();
      serverStarter.before();
      return serverStarter;
    });
    members[index] = new MemberVM(server, serverVM);
    return members[index];
  }

  /**
   * Returns the {@link Member} running inside the VM with the specified {@code index}
   */
  public Member getMember(int index) {
    return members[index];
  }

  public TemporaryFolder getTempFolder() {
    return temporaryFolder;
  }

  private void cleanupVm() {
    if (serverStarter != null) {
      serverStarter.after();
      serverStarter = null;
    }
    if (locatorStarter != null) {
      locatorStarter.after();
      locatorStarter = null;
    }
  }

  private File createWorkingDirForMember(String dirName) throws IOException {
    File workingDir = new File(temporaryFolder.getRoot(), dirName).getAbsoluteFile();
    if (!workingDir.exists()) {
      temporaryFolder.newFolder(dirName);
    }

    return workingDir;
  }
}
