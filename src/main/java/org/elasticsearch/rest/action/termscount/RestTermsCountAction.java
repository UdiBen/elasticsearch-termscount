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
package org.elasticsearch.rest.action.termscount;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.termscount.TermsCountAction;
import org.elasticsearch.action.termscount.TermsCountRequest;
import org.elasticsearch.action.termscount.TermsCountResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

public class RestTermsCountAction extends BaseRestHandler {

    @Inject
    public RestTermsCountAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/_termscount", this);
        controller.registerHandler(POST, "/{index}/_termscount", this);
        controller.registerHandler(POST, "/_termscount/{field}", this);
        controller.registerHandler(POST, "/{index}/_termscount/{field}", this);
        controller.registerHandler(GET, "/_termscount", this);
        controller.registerHandler(GET, "/{index}/_termscount", this);
        controller.registerHandler(GET, "/_termscount/{field}", this);
        controller.registerHandler(GET, "/{index}/_termscount/{field}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        TermsCountRequest termsCountRequest = new TermsCountRequest(RestActions.splitIndices(request.param("index")));
        termsCountRequest.setField(request.param("field"));
        client.execute(TermsCountAction.INSTANCE, termsCountRequest, new ActionListener<TermsCountResponse>() {

            @Override
            public void onResponse(TermsCountResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field("ok", true);
                    buildBroadcastShardsHeader(builder, response);
                    builder.field("terms", response.getTerms());
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}