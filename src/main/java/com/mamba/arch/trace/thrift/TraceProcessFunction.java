package com.mamba.arch.trace.thrift;

import com.mamba.arch.trace.thrift.util.ThriftUtils;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;

class TraceProcessFunction<I, T extends TBase<T, TFieldIdEnum>> extends ProcessFunction<I, TraceStruct<T>> {

    private final ProcessFunction<I, T> function;

    private final boolean oneway;

    public TraceProcessFunction(ProcessFunction<I, T> function) {
        super(function.getMethodName());
        this.function = function;
        this.oneway = ThriftUtils.isOneway(function);
    }

    @Override
    protected boolean isOneway() {
        return this.oneway;
    }

    @Override
    public TBase getResult(I iface, TraceStruct<T> struct) throws TException {
        T data = struct.getData();
        Map<String, String> trace = struct.getTrace();
        //TODO read trace
        System.out.println(trace);  //TODO
        return this.function.getResult(iface, data);
    }

    @Override
    public TraceStruct<T> getEmptyArgsInstance() {
        T args = this.function.getEmptyArgsInstance();
        return new TraceStruct<>(args);
    }
}
