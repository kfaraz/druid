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

package org.apache.druid.java.util.common.parsers;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.TextReader;
import org.apache.druid.java.util.common.collect.Utils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class AbstractFlatTextFormatParser implements Parser<String, Object>
{
  public enum FlatTextFormat
  {
    CSV(","),
    DELIMITED("\t");

    private final String defaultDelimiter;

    FlatTextFormat(String defaultDelimiter)
    {
      this.defaultDelimiter = defaultDelimiter;
    }

    public String getDefaultDelimiter()
    {
      return defaultDelimiter;
    }
  }

  private final String listDelimiter;
  private final Function<String, Object> transformationFunction;
  private final boolean hasHeaderRow;
  private final int maxSkipHeaderRows;

  private List<String> fieldNames = null;
  private boolean hasParsedHeader = false;
  private int skippedHeaderRows;
  private boolean supportSkipHeaderRows;

  public AbstractFlatTextFormatParser(
      @Nullable final String listDelimiter,
      final boolean hasHeaderRow,
      final int maxSkipHeaderRows,
      final boolean tryParseNumbers
  )
  {
    this.listDelimiter = listDelimiter != null ? listDelimiter : Parsers.DEFAULT_LIST_DELIMITER;
    this.transformationFunction = ParserUtils.getTransformationFunction(
        this.listDelimiter,
        Splitter.on(this.listDelimiter),
        tryParseNumbers
    );

    this.hasHeaderRow = hasHeaderRow;
    this.maxSkipHeaderRows = maxSkipHeaderRows;
  }

  @Override
  public void startFileFromBeginning()
  {
    if (hasHeaderRow) {
      fieldNames = null;
    }
    hasParsedHeader = false;
    skippedHeaderRows = 0;
    supportSkipHeaderRows = true;
  }

  public String getListDelimiter()
  {
    return listDelimiter;
  }

  @Override
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @Override
  public void setFieldNames(final Iterable<String> fieldNames)
  {
    if (fieldNames != null) {
      this.fieldNames = TextReader.findOrCreateInputRowSignature(Lists.newArrayList(fieldNames)).getColumnNames();
    }
  }

  public void setFieldNames(final String header)
  {
    try {
      setFieldNames(parseLine(header));
    }
    catch (Exception e) {
      throw new ParseException(header, e, "Unable to parse header [%s]", header);
    }
  }

  @Override
  public Map<String, Object> parseToMap(final String input)
  {
    if (!supportSkipHeaderRows && (hasHeaderRow || maxSkipHeaderRows > 0)) {
      throw new UnsupportedOperationException("hasHeaderRow or maxSkipHeaderRows is not supported. "
                                              + "Please check the indexTask supports these options.");
    }

    try {
      List<String> values = parseLine(input);

      if (skippedHeaderRows < maxSkipHeaderRows) {
        skippedHeaderRows++;
        return null;
      }

      if (hasHeaderRow && !hasParsedHeader) {
        if (fieldNames == null) {
          setFieldNames(values);
        }
        hasParsedHeader = true;
        return null;
      }

      if (fieldNames == null) {
        setFieldNames(ParserUtils.generateFieldNames(values.size()));
      }

      return Utils.zipMapPartial(fieldNames, Iterables.transform(values, transformationFunction));
    }
    catch (Exception e) {
      throw new ParseException(input, e, "Unable to parse row [%s]", input);
    }
  }

  protected abstract List<String> parseLine(String input) throws IOException;
}
