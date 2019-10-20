package com.mamba.arch.trace.thrift.util;

import org.apache.thrift.AsyncProcessFunction;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.async.TAsyncMethodCall;
import org.apache.thrift.transport.TNonblockingTransport;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public final class ThriftUtils {

    private static final Field T_ASYNC_CLIENT_FIELD_MANAGER;

    private static final Field T_ASYNC_CLIENT_FIELD_TRANSPORT;

    private static final Field T_ASYNC_CLIENT_FIELD_CURRENT_METHOD;

    private static final Method T_ASYNC_CLIENT_METHOD_CHECK_READY;

    static {
        try {
            T_ASYNC_CLIENT_FIELD_MANAGER = TAsyncClient.class.getDeclaredField("___manager");
            T_ASYNC_CLIENT_FIELD_MANAGER.setAccessible(true);
            T_ASYNC_CLIENT_FIELD_TRANSPORT = TAsyncClient.class.getDeclaredField("___transport");
            T_ASYNC_CLIENT_FIELD_TRANSPORT.setAccessible(true);
            T_ASYNC_CLIENT_FIELD_CURRENT_METHOD = TAsyncClient.class.getDeclaredField("___currentMethod");
            T_ASYNC_CLIENT_FIELD_CURRENT_METHOD.setAccessible(true);
            T_ASYNC_CLIENT_METHOD_CHECK_READY = TAsyncClient.class.getDeclaredMethod("checkReady");
            T_ASYNC_CLIENT_METHOD_CHECK_READY.setAccessible(true);
        } catch (NoSuchFieldException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    public static TNonblockingTransport getTransport(TAsyncClient client) throws Exception {
        return (TNonblockingTransport) T_ASYNC_CLIENT_FIELD_TRANSPORT.get(client);
    }

    public static void call(TAsyncClient client, TAsyncMethodCall methodCall) throws TException, IllegalAccessException {
        T_ASYNC_CLIENT_FIELD_CURRENT_METHOD.set(client, methodCall);
        TAsyncClientManager manager = (TAsyncClientManager) T_ASYNC_CLIENT_FIELD_MANAGER.get(client);
        manager.call(methodCall);
    }

    public static void checkReady(TAsyncClient client) throws Exception {
        T_ASYNC_CLIENT_METHOD_CHECK_READY.invoke(client);
    }

    public static Class<?> getInnerClass(Class<?> clazz, String name) {
        for (Class<?> innerClass : clazz.getClasses()) {
            if (name.equals(innerClass.getSimpleName())) {
                return innerClass;
            }
        }
        return null;
    }

    public static Map<String, Class<? extends TBase>> getArgsClasses(Class<?> clazz) {
        Map<String, Class<? extends TBase>> argsClasses = new HashMap<>();
        for (Class<?> innerClass : clazz.getClasses()) {
            String name = innerClass.getSimpleName();
            if (TBase.class.isAssignableFrom(innerClass) && name.endsWith("_args")) {
                argsClasses.put(name.substring(0, name.length() - 5), (Class<? extends TBase>) innerClass);
            }
        }
        return argsClasses;
    }

    public static void findMethods(Class<? extends TServiceClient> clazz, Map<String, Method> sendMethods, Map<String, Method> receiveMethods) {
        for (Method method : clazz.getDeclaredMethods()) {
            String name = method.getName();
            if (name.startsWith("send_")) {
                sendMethods.put(name.substring(5), method);
            } else if (name.startsWith("recv_")) {
                receiveMethods.put(name.substring(5), method);
            }
        }
    }

    public static boolean isOneway(AsyncProcessFunction function) {
        return isOneway((Object) function);
    }

    public static boolean isOneway(ProcessFunction function) {
        return isOneway((Object) function);
    }

    private static boolean isOneway(Object function) {
        try {
            Method method = function.getClass().getDeclaredMethod("isOneway");
            method.setAccessible(true);
            return (Boolean) method.invoke(function);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    public static <K, VF, VT> Map<K, VT> transformValues(Map<K, VF> fromMap, Function<? super VF, VT> mapper) {
        Map<K, VT> toMap = new HashMap<>((int) Math.ceil(fromMap.size() / 0.75));
        for (Map.Entry<K, VF> entry : fromMap.entrySet()) {
            toMap.put(entry.getKey(), mapper.apply(entry.getValue()));
        }
        return toMap;
    }
}
