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

import org.apache.druid.testing.embedded.indexing.Resources;
import org.junit.jupiter.api.Test;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DruidOnK3sIT
{
  @Test
  void runDruidOnK3s() throws Exception
  {
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
      applyManifest(kubeConfigFile, "manifests/druid-router.yaml");

      // Wait for Router pod to be ready
      waitForPod(kubeConfigFile, "druid", "druid-router");

      // Port forward router to localhost:8888
      Process portForward = new ProcessBuilder(
          "kubectl", "--kubeconfig", kubeConfigFile.toString(),
          "-n", "druid", "port-forward", "svc/druid-router", "8888:8888"
      ).inheritIO().start();

      Thread.sleep(15_000); // wait for port forward

      // Test: router should be reachable
      var resp = new URL("http://localhost:8888/status/health")
          .openConnection()
          .getInputStream()
          .readAllBytes();
      String body = new String(resp, StandardCharsets.UTF_8);
      assertTrue(body.contains("true"));

      portForward.destroy();
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
}
