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

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

public class TermsCountAction extends Action<TermsCountRequest, TermsCountResponse, TermsCountRequestBuilder> {

    public static final TermsCountAction INSTANCE = new TermsCountAction();
    public static final String NAME = "indices/termscount";

    private TermsCountAction() {
        super(NAME);
    }

    @Override
    public TermsCountResponse newResponse() {
        return new TermsCountResponse();
    }

    @Override
    public TermsCountRequestBuilder newRequestBuilder(Client client) {
        return new TermsCountRequestBuilder(client);
    }
}
