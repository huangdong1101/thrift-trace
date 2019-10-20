package com.mamba.arch.trace.thrift;

import com.mamba.arch.trace.thrift.util.ThriftUtils;
import org.apache.thrift.TBaseProcessor;

import java.lang.reflect.Field;

public class TraceProcessor<I> extends TBaseProcessor<I> {

    public TraceProcessor(TBaseProcessor<I> processor) {
        super(getIface(processor), ThriftUtils.transformValues(processor.getProcessMapView(), TraceProcessFunction::new));
    }

    private static <I> I getIface(TBaseProcessor<I> processor) {
        try {
            Field field = TBaseProcessor.class.getDeclaredField("iface");
            field.setAccessible(true);
            Object iface = field.get(processor);
            return (I) iface;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }
}
