package com.mamba.arch.trace.thrift;

import org.apache.thrift.TBaseProcessor;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TraceProcessor<I> extends TBaseProcessor<I> {

    public TraceProcessor(TBaseProcessor<I> processor) {
        super(getIface(processor), transformValues(processor.getProcessMapView(), TraceProcessFunction::new));
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

    private static <K, VF, VT> Map<K, VT> transformValues(Map<K, VF> fromMap, Function<? super VF, VT> mapper) {
        Map<K, VT> toMap = new HashMap<>((int) Math.ceil(fromMap.size() / 0.75));
        for (Map.Entry<K, VF> entry : fromMap.entrySet()) {
            toMap.put(entry.getKey(), mapper.apply(entry.getValue()));
        }
        return toMap;
    }
}
