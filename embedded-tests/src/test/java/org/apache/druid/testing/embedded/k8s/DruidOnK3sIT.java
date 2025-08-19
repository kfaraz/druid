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

package org.apache.druid.testing.embedded.k8s;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.junit.jupiter.api.Test;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DruidOnK3sIT
{
  @Test
  void runDruidOnK3s() throws Exception
  {
    final List<Process> portForwards = new ArrayList<>();
    try (K3sContainer k3s = new K3sContainer(DockerImageName.parse("rancher/k3s:v1.30.4-k3s1"))) {
      k3s.start();

      // Save kubeconfig to a temp file
      String kubeConfigYaml = k3s.getKubeConfigYaml();
      Path kubeConfigFile = Files.createTempFile("k3s-", ".yaml");
      Files.writeString(kubeConfigFile, kubeConfigYaml);

      // Apply Druid manifests
      applyManifest(kubeConfigFile, "manifests/druid-namespace.yaml");
      applyManifest(kubeConfigFile, "manifests/druid-postgres.yaml");
      applyManifest(kubeConfigFile, "manifests/druid-zookeeper.yaml");
      applyManifest(kubeConfigFile, "manifests/druid-common-configmap.yaml");
      applyManifest(kubeConfigFile, "manifests/druid-coordinator-overlord.yaml");
      applyManifest(kubeConfigFile, "manifests/druid-broker.yaml");
      applyManifest(kubeConfigFile, "manifests/druid-historical.yaml");
      applyManifest(kubeConfigFile, "manifests/druid-middlemanager.yaml");
      applyManifest(kubeConfigFile, "manifests/druid-router.yaml");

      // Wait for Router pod to be ready
      waitForPod(kubeConfigFile, "druid", "druid-router");

      loadDruidImageToContainer();

      // Forward all the required ports
      portForwards.add(startPortForwarding(kubeConfigFile, "svc/druid-router", "8888:8888"));
      // portForwards.add(startPortForwarding(kubeConfigFile, "svc/druid-coordinator-overlord", "8081:8081"));
//      portForwards.add(startPortForwarding(kubeConfigFile, "svc/druid-middlemanager", "8091:8091"));
//      portForwards.add(startPortForwarding(kubeConfigFile, "svc/druid-broker", "8082:8082"));
//      portForwards.add(startPortForwarding(kubeConfigFile, "svc/druid-historical", "8083:8083"));

      Thread.sleep(15_000); // wait for port forward

      // Test: router should be reachable
      byte[] resp = new URL("http://localhost:8888/status/health")
          .openConnection()
          .getInputStream()
          .readAllBytes();
      String body = new String(resp, StandardCharsets.UTF_8);
      assertTrue(body.contains("true"));
    }
    finally {
      portForwards.forEach(Process::destroy);
    }
  }

  private void applyManifest(Path kubeConfig, String manifest) throws Exception
  {
    final String manifestResource = Resources.getFileForResource(manifest).getAbsolutePath();
    Process p = new ProcessBuilder(
        "kubectl", "--kubeconfig", kubeConfig.toString(),
        "apply", "-f", manifestResource
    ).inheritIO().start();
    if (p.waitFor() != 0) {
      throw new RuntimeException("Failed to apply " + manifest);
    }
  }

  private void waitForPod(Path kubeConfig, String namespace, String appLabel) throws Exception
  {
    for (int i = 0; i < 60; i++) {
      Process p = new ProcessBuilder(
          "kubectl", "--kubeconfig", kubeConfig.toString(),
          "-n", namespace, "get", "pods", "-l", "app=" + appLabel,
          "-o", "jsonpath={.items[0].status.containerStatuses[0].ready}"
      ).redirectErrorStream(true).start();

      String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();

      if ("true".equals(out)) {
        return;
      }
      Thread.sleep(5000);
    }
    throw new RuntimeException("Pod " + appLabel + " not ready in time");
  }

  private Process startPortForwarding(Path kubeConfigFile, String service, String portMapping) throws IOException
  {
    return new ProcessBuilder(
        "kubectl", "--kubeconfig", kubeConfigFile.toString(),
        "-n", "druid", "port-forward", service, portMapping
    ).inheritIO().start();
  }

  private void loadDruidImageToContainer() throws IOException, InterruptedException
  {
//    final String imageName = "apache/druid:33.0.0";
//    final String tarPath = new File(FileUtils.createTempDir(), "tar").getAbsolutePath();
//    ProcessBuilder listLocalProcess = new ProcessBuilder("docker", "images", "--format", "table {{.Repository}}:{{.Tag}}");
//    listLocalProcess.start().waitFor();
//
//    ProcessBuilder saveProcess = new ProcessBuilder("docker", "save", "-o", tarPath, imageName);
//    saveProcess.start().waitFor();
//
//    ProcessBuilder loadProcess = new ProcessBuilder("docker", "exec", containerId, "ctr", "-n", "k8s.io", "images", "import", "/tmp/druid-image.tar");
//    loadProcess.start().waitFor();
//
//    ProcessBuilder listProcess = new ProcessBuilder("docker", "exec", containerId, "ctr", "-n", "k8s.io", "images", "list");
//    listProcess.start().waitFor();
  }
}
