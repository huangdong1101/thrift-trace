package com.mamba.sample.impl;

import com.mamba.arch.trace.thrift.TraceProcessor;
import com.mamba.sample.face.SharedService;
import com.mamba.sample.face.SharedStructIn;
import com.mamba.sample.face.SharedStructOut;
import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

class SharedServiceImpl implements SharedService.Iface {

    private static final Logger LOGGER = LoggerFactory.getLogger(SharedServiceImpl.class);

    @Override
    public List<SharedStructOut> getStruct(int key, String token, SharedStructIn input) throws TException {
        LOGGER.info("process getStruct");
        SharedStructOut out = new SharedStructOut();
        out.key = 1;
        out.value = "test";
        return Collections.singletonList(out);
    }

    @Override
    public void getStruct1(int key, String token, SharedStructIn input) throws TException {
        LOGGER.info("process getStruct1");
    }

    public static void main(String[] args) throws TTransportException {
//        serve(8089, false);
        serve(8089, true);
    }

    private static void serve(int port, boolean trace) throws TTransportException {
        try (TNonblockingServerSocket socket = new TNonblockingServerSocket(port)) {
            LOGGER.info("=========Thrift server starting=======");
            LOGGER.info("Listen port: {}", port);

            SharedServiceImpl service = new SharedServiceImpl();
            TBaseProcessor<SharedServiceImpl> processor = new SharedService.Processor<>(service);
            if (trace) {
                processor = new TraceProcessor<>(processor);
            }

            TNonblockingServer.Args arg = new TNonblockingServer.Args(socket);
            arg.protocolFactory(new TCompactProtocol.Factory());
            arg.transportFactory(new TFramedTransport.Factory());
            arg.processorFactory(new TProcessorFactory(processor));
            TServer server = new TNonblockingServer(arg);

            LOGGER.info("=========Thrift server started=======");
            server.serve();
            LOGGER.error("Thrift server stopped as an error happened");
        }
    }
}
