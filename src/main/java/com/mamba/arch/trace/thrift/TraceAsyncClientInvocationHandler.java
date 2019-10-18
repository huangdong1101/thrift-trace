package com.mamba.arch.trace.thrift;

import com.mamba.arch.trace.thrift.util.ThriftUtils;
import lombok.Getter;
import org.apache.thrift.TBase;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.protocol.TProtocol;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class TraceAsyncClientInvocationHandler<T extends TAsyncClient> implements InvocationHandler {

    private final Supplier<T> asyncClientSupplier;

    private final Method sendBaseMethod;

    private final Map<String, MethodDesc> proxyMethods;

    public TraceAsyncClientInvocationHandler(Supplier<T> asyncClientSupplier, Class<T> clazz) throws Exception {
        Objects.requireNonNull(clazz);
        this.asyncClientSupplier = Objects.requireNonNull(asyncClientSupplier);

        this.sendBaseMethod = TServiceClient.class.getDeclaredMethod("sendBase", String.class, TBase.class);
        this.sendBaseMethod.setAccessible(true);

        Map<String, MethodDesc> proxyMethods = getServiceMethodDescMap(clazz);
        this.proxyMethods = Collections.unmodifiableMap(proxyMethods);
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();
        T asyncClient = this.asyncClientSupplier.get();
        MethodDesc methodDesc = this.proxyMethods.get(methodName);
        if (methodDesc == null) {
            return method.invoke(asyncClient, args);
        }

        //checkReady
        ThriftUtils.checkReady(asyncClient);

        //gen async method call
        AsyncMethodCallback callback = (AsyncMethodCallback) args[args.length - 1];
        TBase methodArgs = (TBase) methodDesc.getArgsConstructor().newInstance(Arrays.copyOf(args, args.length - 1));
        TraceAsyncMethodCall<?> methodCall = new TraceAsyncMethodCall<>(asyncClient, methodName, methodArgs, callback, methodDesc.getReceiveFunction());

        //do call
        ThriftUtils.call(asyncClient, methodCall);
        return null;
    }

    public <I> I newProxyInstance(Class<I> interfaceClass) {
        return (I) Proxy.newProxyInstance(ClassLoader.getSystemClassLoader(), new Class<?>[]{interfaceClass}, this);
    }

    private static <T extends TAsyncClient> Map<String, MethodDesc> getServiceMethodDescMap(Class<T> clazz) throws Exception {
        Map<String, Class<? extends TBase>> argsClasses = ThriftUtils.getArgsClasses(clazz.getDeclaringClass());
        Class<? extends TServiceClient> serviceClient = (Class<? extends TServiceClient>) ThriftUtils.getInnerClass(clazz.getDeclaringClass(), "Client");
        Class<? extends TServiceClientFactory> serviceClientFactoryClass = (Class<? extends TServiceClientFactory>) ThriftUtils.getInnerClass(serviceClient, "Factory");

        Map<String, Method> sendMethods = new HashMap<>();
        Map<String, Method> receiveMethods = new HashMap<>();
        ThriftUtils.findMethods(serviceClient, sendMethods, receiveMethods);

        TServiceClientFactory serviceClientFactory = serviceClientFactoryClass.newInstance();

        Map<String, MethodDesc> proxyMethods = new HashMap<>((int) Math.ceil(argsClasses.size() / 0.75));
        for (Map.Entry<String, Class<? extends TBase>> entry : argsClasses.entrySet()) {
            String name = entry.getKey();
            Method sendMethod = sendMethods.get(name);
            Method receiveMethod = receiveMethods.get(name);

            Class<? extends TBase> argsClass = entry.getValue();
            Constructor<? extends TBase> argsConstructor = argsClass.getConstructor(sendMethod.getParameterTypes());
            if (receiveMethod == null) {
                proxyMethods.put(name, new MethodDesc(argsConstructor));
            } else {
                Function<TProtocol, Object> receiveFunction = protocol -> invokeReceiveMethod(protocol, serviceClientFactory, receiveMethod);
                proxyMethods.put(name, new MethodDesc(argsConstructor, receiveFunction));
            }
        }
        return proxyMethods;
    }

    private static Object invokeReceiveMethod(TProtocol protocol, TServiceClientFactory serviceClientFactory, Method receiveMethod) {
        TServiceClient serviceClient = serviceClientFactory.getClient(protocol);
        try {
            return receiveMethod.invoke(serviceClient);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    @Getter
    private static class MethodDesc<T extends TBase, R> {

        private final Constructor<T> argsConstructor;

        private final Function<TProtocol, R> receiveFunction;

        public MethodDesc(Constructor<T> argsConstructor) {
            this(argsConstructor, null);
        }

        public MethodDesc(Constructor<T> argsConstructor, Function<TProtocol, R> receiveFunction) {
            this.argsConstructor = argsConstructor;
            this.receiveFunction = receiveFunction;
        }
    }
}
