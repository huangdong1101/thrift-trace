package com.mamba.arch.trace.thrift;

import org.apache.thrift.TServiceClient;

public interface ServiceClientFactory<T extends TServiceClient> {

    T get() throws Exception;

    void destroy(T client);
}
