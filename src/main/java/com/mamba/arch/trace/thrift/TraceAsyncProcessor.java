package com.mamba.arch.trace.thrift;

import com.mamba.arch.trace.thrift.util.ThriftUtils;
import org.apache.thrift.TBaseAsyncProcessor;

import java.lang.reflect.Field;

public class TraceAsyncProcessor<I> extends TBaseAsyncProcessor<I> {

    public TraceAsyncProcessor(TBaseAsyncProcessor<I> processor) {
        super(getIface(processor), ThriftUtils.transformValues(processor.getProcessMapView(), TraceAsyncProcessFunction::new));
    }

    private static <I> I getIface(TBaseAsyncProcessor<I> processor) {
        try {
            Field field = TBaseAsyncProcessor.class.getDeclaredField("iface");
            field.setAccessible(true);
            Object iface = field.get(processor);
            return (I) iface;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }
}
