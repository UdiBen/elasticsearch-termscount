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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.BaseRequestBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.Client;

/**
 * A request to get termlists of one or more indices.
 */
public class TermsCountRequestBuilder extends BaseRequestBuilder<TermsCountRequest, TermsCountResponse> {

    public TermsCountRequestBuilder(Client indicesClient) {
        super(indicesClient, new TermsCountRequest());
    }

    public TermsCountRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    public TermsCountRequestBuilder setListenerThreaded(boolean threadedListener) {
        request.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * Controls the operation threading model.
     */
    public TermsCountRequestBuilder setOperationThreading(BroadcastOperationThreading operationThreading) {
        request.operationThreading(operationThreading);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<TermsCountResponse> listener) {
        client.execute(TermsCountAction.INSTANCE, request, listener);
    }
}
