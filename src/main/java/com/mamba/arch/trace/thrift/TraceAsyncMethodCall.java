package com.mamba.arch.trace.thrift;

import com.mamba.arch.trace.thrift.util.ThriftUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncMethodCall;
import org.apache.thrift.protocol.TProtocol;

import java.util.Collections;
import java.util.function.Function;

class TraceAsyncMethodCall<R> extends TAsyncMethodCall<R> {

    private final String methodName;

    private final TBase methodArgs;

    private final Function<TProtocol, R> receiveFunction;

    public TraceAsyncMethodCall(TAsyncClient client, String methodName, TBase methodArgs, AsyncMethodCallback<R> callback, Function<TProtocol, R> receiveFunction) throws Exception {
        super(client, client.getProtocolFactory(), ThriftUtils.getTransport(client), callback, false);
        this.methodName = methodName;
        this.methodArgs = methodArgs;
        this.receiveFunction = receiveFunction;
    }

    @Override
    protected void write_args(TProtocol prot) throws TException {
        prot.writeMessageBegin(new org.apache.thrift.protocol.TMessage(methodName, org.apache.thrift.protocol.TMessageType.CALL, 0));

        //wrap Args, set trace
        TraceStruct<?> proxyArgs = new TraceStruct<>(methodArgs);
        //TODO write trace
        proxyArgs.setTrace(Collections.singletonMap("tid", "1234")); //TODO
        proxyArgs.write(prot);

        prot.writeMessageEnd();
    }

    @Override
    protected R getResult() throws Exception {
        if (getState() != org.apache.thrift.async.TAsyncMethodCall.State.RESPONSE_READ) {
            throw new java.lang.IllegalStateException("Method call not finished!");
        }
        org.apache.thrift.transport.TMemoryInputTransport memoryTransport = new org.apache.thrift.transport.TMemoryInputTransport(getFrameBuffer().array());
        org.apache.thrift.protocol.TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
        if (this.receiveFunction == null) {
            return null;
        }
        return this.receiveFunction.apply(prot);
    }
}
