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

package org.apache.druid.client;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JsonParserIterator<T> implements CloseableIterator<T>
{
  private static final Logger LOG = new Logger(JsonParserIterator.class);

  private JsonParser jp;
  private ObjectCodec objectCodec;
  private final JavaType typeRef;
  private final Future<InputStream> future;
  private final String url;
  private final String host;
  private final ObjectMapper objectMapper;
  private final boolean hasTimeout;
  private final long timeoutAt;
  private final String queryId;

  public JsonParserIterator(
      JavaType typeRef,
      Future<InputStream> future,
      String url,
      @Nullable Query<T> query,
      String host,
      ObjectMapper objectMapper
  )
  {
    this.typeRef = typeRef;
    this.future = future;
    this.url = url;
    if (query != null) {
      this.timeoutAt = query.context().getLong(DirectDruidClient.QUERY_FAIL_TIME, -1L);
      this.queryId = query.getId();
    } else {
      this.timeoutAt = -1;
      this.queryId = null;
    }
    this.jp = null;
    this.host = host;
    this.objectMapper = objectMapper;
    this.hasTimeout = timeoutAt > -1;
  }

  /**
   * Bypasses Jackson serialization to prevent materialization of results from the {@code future} in memory at once.
   * A shortened version of {@link #JsonParserIterator(JavaType, Future, String, Query, String, ObjectMapper)}
   * where the URL and host parameters, used solely for logging/errors, are not known.
   */
  public JsonParserIterator(JavaType typeRef, Future<InputStream> future, ObjectMapper objectMapper)
  {
    this(typeRef, future, "", null, "", objectMapper);
  }

  @Override
  public boolean hasNext()
  {
    init();

    if (jp.isClosed()) {
      return false;
    }
    if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
      CloseableUtils.closeAndWrapExceptions(jp);
      return false;
    }

    return true;
  }

  @Override
  public T next()
  {
    init();

    try {
      final T retVal = objectCodec.readValue(jp, typeRef);
      jp.nextToken();
      return retVal;
    }
    catch (IOException e) {
      // check for timeout, a failure here might be related to a timeout, so lets just attribute it
      if (checkTimeout()) {
        QueryTimeoutException timeoutException = timeoutQuery();
        timeoutException.addSuppressed(e);
        throw timeoutException;
      } else {
        throw convertException(e);
      }
    }
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException
  {
    if (jp != null) {
      jp.close();
    }
  }

  private boolean checkTimeout()
  {
    long timeLeftMillis = timeoutAt - System.currentTimeMillis();
    return checkTimeout(timeLeftMillis);
  }

  private boolean checkTimeout(long timeLeftMillis)
  {
    if (hasTimeout && timeLeftMillis < 1) {
      return true;
    }
    return false;
  }

  private void init()
  {
    if (jp == null) {
      try {
        long timeLeftMillis = timeoutAt - System.currentTimeMillis();
        if (checkTimeout(timeLeftMillis)) {
          throw timeoutQuery();
        }
        InputStream is = hasTimeout ? future.get(timeLeftMillis, TimeUnit.MILLISECONDS) : future.get();

        if (is != null) {
          jp = objectMapper.getFactory().createParser(is);
        } else if (checkTimeout()) {
          throw timeoutQuery();
        } else {
          // The InputStream is null and we have not timed out, there might be multiple reasons why we could hit this
          // condition, guess that we are hitting it because of scatter-gather bytes.  It would be better to be more
          // explicit about why errors are happening than guessing, but this comment is being rewritten from a T-O-D-O,
          // so the intent is just to document this better rather than do all of the logic to fix it.  If/when we get
          // this exception thrown for other reasons, it would be great to document what other reasons this can happen.
          throw ResourceLimitExceededException.withMessage(
              "Possibly max scatter-gather bytes limit reached while reading from url[%s].",
              url
          );
        }

        final JsonToken nextToken = jp.nextToken();
        if (nextToken == JsonToken.START_ARRAY) {
          jp.nextToken();
          objectCodec = jp.getCodec();
        } else if (nextToken == JsonToken.START_OBJECT) {
          throw convertException(jp.getCodec().readValue(jp, QueryException.class));
        } else {
          String errMsg = jp.getValueAsString();
          if (errMsg != null) {
            errMsg = errMsg.substring(0, Math.min(errMsg.length(), 192));
          }
          throw convertException(
              new IAE(
                  "Next token wasn't a START_ARRAY, was[%s] from url[%s] with value[%s]",
                  jp.getCurrentToken(),
                  url,
                  errMsg
              )
          );
        }
      }
      catch (ExecutionException | CancellationException e) {
        throw convertException(e.getCause() == null ? e : e.getCause());
      }
      catch (IOException | InterruptedException e) {
        throw convertException(e);
      }
      catch (TimeoutException e) {
        throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out!", queryId), host);
      }
    }
  }

  private QueryTimeoutException timeoutQuery()
  {
    return new QueryTimeoutException(StringUtils.nonStrictFormat("url[%s] timed out", url), host);
  }

  /**
   * Converts the given exception to a proper type of {@link QueryException}.
   * The use cases of this method are:
   * <p>
   * - All non-QueryExceptions are wrapped with {@link QueryInterruptedException}.
   * - The QueryException from {@link DirectDruidClient} is converted to a more specific type of QueryException
   * based on {@link QueryException#getErrorCode()}. During conversion, {@link QueryException#host} is overridden
   * by {@link #host}.
   */
  private QueryException convertException(Throwable cause)
  {
    LOG.warn(cause, "Query [%s] to host [%s] interrupted", queryId, host);
    if (cause instanceof QueryException) {
      final QueryException queryException = (QueryException) cause;
      if (queryException.getErrorCode() == null) {
        // errorCode should not be null now, but maybe could be null in the past...
        return new QueryInterruptedException(
            QueryException.UNKNOWN_EXCEPTION_ERROR_CODE,
            queryException.getMessage(),
            queryException.getErrorClass(),
            host
        );
      }

      // Note: this switch clause is to restore the 'type' information of QueryExceptions which is lost during
      // JSON serialization. As documented on the QueryException class, the errorCode of QueryException is the only
      // way to differentiate the cause of the exception.  This code does not cover all possible exceptions that
      // could come up and so, likely, doesn't produce exceptions reliably.  The only safe way to catch and interact
      // with a QueryException is to catch QueryException and check its errorCode.  In some future code change, we
      // should likely remove this switch entirely, but when we do that, we need to make sure to also adjust any
      // points in the code that are catching the specific child Exceptions to instead catch QueryException and
      // check the errorCode.
      switch (queryException.getErrorCode()) {
        // The below is the list of exceptions that can be thrown in historicals and propagated to the broker.
        case QueryException.QUERY_TIMEOUT_ERROR_CODE:
          return new QueryTimeoutException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              host
          );
        case QueryException.QUERY_CAPACITY_EXCEEDED_ERROR_CODE:
          return new QueryCapacityExceededException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              host
          );
        case QueryException.QUERY_UNSUPPORTED_ERROR_CODE:
          return new QueryUnsupportedException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              host
          );
        case QueryException.RESOURCE_LIMIT_EXCEEDED_ERROR_CODE:
          return new ResourceLimitExceededException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              host
          );
        default:
          return new QueryInterruptedException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              host
          );
      }
    } else {
      return new QueryInterruptedException(cause, host);
    }
  }
}
