package com.mamba.arch.trace.thrift;

import com.mamba.sample.face.SharedService;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TraceServiceClientInvocationHandlerTests {

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void newProxyInstance() throws Exception {
        ServiceClientFactory<SharedService.Client> serviceClientFactory = new ServiceClientFactory<SharedService.Client>() {
            @Override
            public SharedService.Client get() throws Exception {
                TSocket socket = new TSocket("0.0.0.0", 8089, 1000 * 10);
                socket.open();
                return new SharedService.Client(new TCompactProtocol(new TFramedTransport(socket)));
            }

            @Override
            public void destroy(SharedService.Client client) {
                client.getInputProtocol().getTransport().close();
            }
        };
        TraceServiceClientInvocationHandler<SharedService.Client> invocationHandler = new TraceServiceClientInvocationHandler<>(serviceClientFactory, SharedService.Client.class);
        SharedService.Iface iface = invocationHandler.newProxyInstance(SharedService.Iface.class);
        iface.getStruct1(11, "xxx123", null);
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