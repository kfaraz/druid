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

package org.apache.druid.testing.utils;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ITRetryUtil
{

  private static final Logger LOG = new Logger(ITRetryUtil.class);

  public static final int DEFAULT_RETRY_COUNT = 240; // 20 minutes

  public static final long DEFAULT_RETRY_SLEEP = TimeUnit.SECONDS.toMillis(5);

  public static void retryUntilTrue(Callable<Boolean> callable, String task)
  {
    retryUntil(callable, true, DEFAULT_RETRY_SLEEP, DEFAULT_RETRY_COUNT, task);
  }

  public static void retryUntilFalse(Callable<Boolean> callable, String task)
  {
    retryUntil(callable, false, DEFAULT_RETRY_SLEEP, DEFAULT_RETRY_COUNT, task);
  }

  public static <T> void retryUntilEquals(
      Callable<T> callable,
      T expectedValue,
      String taskMessageFormat,
      Object... args
  ) {
    retryUntilEquals(
        callable,
        expectedValue,
        DEFAULT_RETRY_SLEEP,
        DEFAULT_RETRY_COUNT,
        StringUtils.format(taskMessageFormat, args)
    );
  }

  public static void retryUntil(
      Callable<Boolean> callable,
      boolean expectedValue,
      long delayInMillis,
      int retryCount,
      String taskMessage
  )
  {
    retryUntilEquals(callable, expectedValue, delayInMillis, retryCount, taskMessage);
  }

  public static <T> void retryUntilEquals(
      Callable<T> callable,
      T expectedValue,
      long delayInMillis,
      int retryCount,
      String taskMessageFormat,
      Object... args
  )
  {
    int currentTry = 0;
    Exception lastException = null;
    final String taskMessage = StringUtils.format(taskMessageFormat, args);

    while (true) {
      try {
        if (currentTry > retryCount) {
          break;
        }

        LOG.info(
            "Trying attempt[%d/%d] of task [%s] with expected value [%s].",
            currentTry, retryCount, taskMessage, expectedValue
        );
        final T observedValue = callable.call();
        if (Objects.equals(observedValue, expectedValue)) {
          break;
        } else {
          LOG.info(
              "Attempt[%d/%d] failed. Task[%s] returned value [%s] but expected [%s]. Next retry in [%d]ms.",
              currentTry, retryCount, taskMessage, observedValue, expectedValue, delayInMillis
          );
        }
      }
      catch (Exception e) {
        // just continue retrying if there is an exception (it may be transient!) but save the last:
        lastException = e;
      }

      try {
        Thread.sleep(delayInMillis);
      }
      catch (InterruptedException e) {
        // Ignore
      }
      currentTry++;
    }

    if (currentTry > retryCount) {
      if (lastException != null) {
        throw new ISE(
            "Max number of retries[%d] exceeded for Task[%s]. Failing.",
            retryCount,
            taskMessage,
            lastException
        );
      } else {
        throw new ISE(
            "Max number of retries[%d] exceeded for Task[%s]. Failing.",
            retryCount,
            taskMessage
        );
      }
    }
  }

}
