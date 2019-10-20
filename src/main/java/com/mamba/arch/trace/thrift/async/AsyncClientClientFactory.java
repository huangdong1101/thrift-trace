package com.mamba.arch.trace.thrift.async;

import org.apache.thrift.async.TAsyncClient;

public interface AsyncClientClientFactory<T extends TAsyncClient> {

    T get() throws Exception;
}
