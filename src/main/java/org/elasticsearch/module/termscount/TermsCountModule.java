package org.elasticsearch.module.termscount;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.termscount.TermsCountAction;
import org.elasticsearch.action.termscount.TransportTermsCountAction;

public class TermsCountModule extends ActionModule {

    public TermsCountModule() {
        super(true);
    }
    
    @Override
    protected void configure() {
        registerAction(TermsCountAction.INSTANCE, TransportTermsCountAction.class);
    }
}
