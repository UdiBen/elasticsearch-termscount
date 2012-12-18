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

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A response for terms action.
 */
public class TermsCountResponse extends BroadcastOperationResponse {

    private Map<String, Integer> terms;

    TermsCountResponse() {
    }

    TermsCountResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, Map<String, Integer> terms) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.terms = terms;
    }

    public Map<String, Integer> getTerms() {
        return terms;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int n = in.readInt();
        terms = new HashMap<String, Integer>();
        for (int i = 0; i < n; i++) {
            String term = in.readUTF();
            int count = 0;
            if (terms.containsKey(term))
                count = terms.get(term);
            terms.put(term, ++count);

        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(terms.size());
        for (Map.Entry<String, Integer> entry : terms.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }
}