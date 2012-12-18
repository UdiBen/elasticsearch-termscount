package org.elasticsearch.plugin.termscount;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.termscount.TermsCountAction;
import org.elasticsearch.action.termscount.TransportTermsCountAction;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.rest.action.termscount.RestTermsCountAction;

public class TermsCountPlugin extends AbstractPlugin {

    @Override 
    public String name() {
        return "termscount";
    }

    @Override 
    public String description() {
        return "Index terms count for Elasticsearch";
    }
    
    public void onModule(RestModule module) {
        module.addRestAction(RestTermsCountAction.class);
    }

    public void onModule(ActionModule module) {
        module.registerAction(TermsCountAction.INSTANCE, TransportTermsCountAction.class);
    }
    
}
