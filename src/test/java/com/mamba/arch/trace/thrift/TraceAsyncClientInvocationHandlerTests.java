package com.mamba.arch.trace.thrift;

import com.mamba.sample.face.SharedService;
import com.mamba.sample.face.SharedStructOut;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Supplier;

class TraceAsyncClientInvocationHandlerTests {

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void newProxyInstance() throws Exception {
        AsyncClientClientFactory<SharedService.AsyncClient> serviceClientFactory = () -> {
            TNonblockingSocket socket = new TNonblockingSocket("0.0.0.0", 8089, 1000 * 10);
            return new SharedService.AsyncClient(new TCompactProtocol.Factory(), new TAsyncClientManager(), socket);
        };

        TraceAsyncClientInvocationHandler<SharedService.AsyncClient> invocationHandler = new TraceAsyncClientInvocationHandler<>(serviceClientFactory, SharedService.AsyncClient.class);
        SharedService.AsyncClient iface = invocationHandler.newProxyInstance(SharedService.AsyncClient.class);
        iface.getStruct(11, "xxx123", null, new AsyncMethodCallback<List<SharedStructOut>>() {
            @Override
            public void onComplete(List<SharedStructOut> sharedStructOutList) {

            }

            @Override
            public void onError(Exception e) {

            }
        });
        System.out.println();
    }

    @Test
    void newProxyInstance1() throws Exception {
        try (TSocket socket = new TSocket("0.0.0.0", 8089, 1000 * 10)) {
            socket.open();
            SharedService.Client client = new SharedService.Client(new TCompactProtocol(new TFramedTransport(socket)));
            client.getStruct1(11, "xxx123", null);
            System.out.println();
        }
    }
}