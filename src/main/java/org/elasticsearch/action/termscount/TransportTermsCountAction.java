/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.termscount;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.common.collect.Lists.newArrayList;

/**
 * Termlist index/indices action.
 */
public class TransportTermsCountAction
        extends TransportBroadcastOperationAction<TermsCountRequest, TermsCountResponse, ShardTermsCountRequest, ShardTermsCountResponse> {

    private final IndicesService indicesService;
    private final Object termlistMutex = new Object();

    @Inject
    public TransportTermsCountAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                     TransportService transportService, IndicesService indicesService) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MERGE;
    }

    @Override
    protected String transportAction() {
        return TermsCountAction.NAME;
    }

    @Override
    protected TermsCountRequest newRequest() {
        return new TermsCountRequest();
    }

    @Override
    protected boolean ignoreNonActiveExceptions() {
        return true;
    }

    @Override
    protected TermsCountResponse newResponse(TermsCountRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        Map<String, Integer> terms = new HashMap<String, Integer>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // a non active shard, ignore...
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                successfulShards++;
                if (shardResponse instanceof ShardTermsCountResponse) {
                    ShardTermsCountResponse resp = (ShardTermsCountResponse) shardResponse;
                    terms.putAll(resp.getTermList());
                }
            }
        }
        return new TermsCountResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, terms);
    }

    @Override
    protected ShardTermsCountRequest newShardRequest() {
        return new ShardTermsCountRequest();
    }

    @Override
    protected ShardTermsCountRequest newShardRequest(ShardRouting shard, TermsCountRequest request) {
        return new ShardTermsCountRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected ShardTermsCountResponse newShardResponse() {
        return new ShardTermsCountResponse();
    }

    @Override
    protected ShardTermsCountResponse shardOperation(ShardTermsCountRequest request) throws ElasticSearchException {
        synchronized (termlistMutex) {
            IndexShard indexShard = indicesService.indexServiceSafe(request.index()).shardSafe(request.shardId());
            Engine.Searcher searcher = indexShard.searcher();
            try {
                Map<String, Integer> set = new HashMap<String, Integer>();
                TermEnum te = searcher.reader().terms();
                do {
                    Term t = te.term();
                    if (t != null && t.field().charAt(0) != '_') {
                        if (request.getField() == null || t.field().equals(request.getField())) {
                            String term = t.text();
                            Integer count = set.get(term);
                            if (count == null)
                                count = 0;
                            set.put(term, ++count);
                        }
                    }
                } while (te.next());
                te.close();
                return new ShardTermsCountResponse(request.index(), request.shardId(), set);
            } catch (IOException ex) {
                throw new ElasticSearchException(ex.getMessage(), ex);
            }
        }
    }

    /**
     * The termlist request works against primary shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, TermsCountRequest request, String[] concreteIndices) {
        return clusterState.routingTable().activePrimaryShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, TermsCountRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, TermsCountRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }
}