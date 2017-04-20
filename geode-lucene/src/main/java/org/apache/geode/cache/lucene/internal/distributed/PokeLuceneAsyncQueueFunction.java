/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.distributed;

import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderEventProcessor;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

public class PokeLuceneAsyncQueueFunction implements Function, InternalEntity {
  private static final long serialVersionUID = 1L;
  public static final String ID = PokeLuceneAsyncQueueFunction.class.getName();

  private static final Logger logger = LogService.getLogger();

  @Override
  public void execute(FunctionContext context) {
    Object[] args = (Object[]) context.getArguments();
    queueLocally(context, (String) args[0], args[1], (GatewaySenderEventImpl) args[2]);
  }

  protected void queueLocally(FunctionContext context, String regionName, Object key,
      GatewaySenderEventImpl event) {
    // Get the AsyncEventQueue
    RegionFunctionContext ctx = (RegionFunctionContext) context;

    PartitionedRegion pr = (PartitionedRegion) ctx.getDataSet();
    Cache cache = pr.getCache();
    String queueId = (String) pr.getAttributes().getAsyncEventQueueIds().iterator().next();
    AsyncEventQueueImpl queue = (AsyncEventQueueImpl) cache.getAsyncEventQueue(queueId);

    // Get the GatewaySender
    AbstractGatewaySender sender = (AbstractGatewaySender) queue.getSender();

    // Update the shadow key
    BucketRegion br = pr.getBucketRegion(key);
    if (br.getBucketAdvisor().isPrimary()) {
      try {
        List<ParallelGatewaySenderEventProcessor> processors =
            ((ConcurrentParallelGatewaySenderEventProcessor) sender.getEventProcessor())
                .getProcessors();
        ParallelGatewaySenderEventProcessor processor =
            processors.get(event.getBucketId() % sender.getDispatcherThreads());
        processor.getQueue().put(event);
      } catch (InterruptedException e) {

      }
    }
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public boolean hasResult() {
    return false;
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }
}
