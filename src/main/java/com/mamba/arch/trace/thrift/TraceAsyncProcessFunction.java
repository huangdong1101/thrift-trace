package com.mamba.arch.trace.thrift;

import org.apache.thrift.AsyncProcessFunction;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;


public class TraceAsyncProcessFunction<I, T extends TBase<T, TFieldIdEnum>, R> extends AsyncProcessFunction<I, TraceStruct<T>, R> {

    private final AsyncProcessFunction<I, T, R> function;

    private final boolean oneway;

    public TraceAsyncProcessFunction(AsyncProcessFunction<I, T, R> function) {
        super(function.getMethodName());
        this.function = function;
        try {
            Method isOnewayMethod = function.getClass().getDeclaredMethod("isOneway");
            isOnewayMethod.setAccessible(true);
            this.oneway = (Boolean) isOnewayMethod.invoke(function);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected boolean isOneway() {
        return this.oneway;
    }

    @Override
    public void start(I iface, TraceStruct<T> args, AsyncMethodCallback<R> resultHandler) throws TException {
        T data = args.getData();
        Map<String, String> trace = args.getTrace();
        //TODO read trace
        System.out.println(trace);  //TODO
        this.function.start(iface, data, resultHandler);
    }

    @Override
    public TraceStruct<T> getEmptyArgsInstance() {
        T args = this.function.getEmptyArgsInstance();
        return new TraceStruct<>(args);
    }

    @Override
    public AsyncMethodCallback<R> getResultHandler(AbstractNonblockingServer.AsyncFrameBuffer fb, int seqid) {
        return this.function.getResultHandler(fb, seqid);
    }
}
