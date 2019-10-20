package com.mamba.arch.trace.thrift;

import org.apache.thrift.async.TAsyncClient;

public interface AsyncClientClientFactory<T extends TAsyncClient> {

    T get() throws Exception;
}
