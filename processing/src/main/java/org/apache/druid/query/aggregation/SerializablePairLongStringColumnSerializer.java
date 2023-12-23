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

package org.apache.druid.query.aggregation;

import com.google.common.base.Preconditions;
import org.apache.druid.segment.serde.cell.ByteBufferProvider;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;

/**
 * valid call sequence
 * <p>
 * open()+serialize()*(getSerializedSize()|writeTo())*
 * <p>
 * getSerializedSize() / writeTo() effectively function as a close call, but each may be called multiple times and has
 * no effect on one another.
 */
public class SerializablePairLongStringColumnSerializer extends AbstractSerializablePairLongObjectColumnSerializer<SerializablePairLongString>
{

  public SerializablePairLongStringColumnSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      ByteBufferProvider byteBufferProvider
  )
  {
    super(new SerializablePairLongStringSimpleStagedSerde(), segmentWriteOutMedium, byteBufferProvider);
  }

  @Override
  public void open() throws IOException
  {
    Preconditions.checkState(state == AbstractSerializablePairLongObjectColumnSerializer.State.START || state == AbstractSerializablePairLongObjectColumnSerializer.State.OPEN, "open called in invalid state %s", state);

    if (state == AbstractSerializablePairLongObjectColumnSerializer.State.START) {
      bufferStore = new SerializablePairLongStringBufferStore(
          new SerializedStorage<>(segmentWriteOutMedium.makeWriteOutBytes(), stagedSerde)
      );
      state = AbstractSerializablePairLongObjectColumnSerializer.State.OPEN;
    }
  }
}
