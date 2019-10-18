package com.mamba.arch.trace.thrift;

import lombok.Getter;
import org.apache.thrift.TBase;
import org.apache.thrift.TServiceClient;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TraceServiceClientInvocationHandler<T extends TServiceClient> implements InvocationHandler {

    private final ServiceClientFactory<T> serviceClientFactory;

    private final Method sendBaseMethod;

    private final Map<String, MethodDesc> proxyMethods;

    public TraceServiceClientInvocationHandler(ServiceClientFactory<T> serviceClientFactory, Class<T> clazz) throws Exception {
        Objects.requireNonNull(clazz);
        this.serviceClientFactory = Objects.requireNonNull(serviceClientFactory);

        this.sendBaseMethod = TServiceClient.class.getDeclaredMethod("sendBase", String.class, TBase.class);
        this.sendBaseMethod.setAccessible(true);

        Map<String, MethodDesc> proxyMethods = getServiceMethodDescMap(clazz);
        this.proxyMethods = Collections.unmodifiableMap(proxyMethods);
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();
        T serviceClient = this.serviceClientFactory.get();
        try {
            MethodDesc<? extends TBase> methodDesc = this.proxyMethods.get(methodName);
            if (methodDesc == null) {
                return method.invoke(serviceClient, args);
            }
            //gen Args Instance
            TBase methodArgs = methodDesc.getArgsConstructor().newInstance(args);
            //wrap Args, set trace
            TraceStruct proxyArgs = new TraceStruct<>(methodArgs);
            //TODO write trace
            proxyArgs.setTrace(Collections.singletonMap("tid", "1234")); //TODO
            //do send
            this.sendBaseMethod.invoke(serviceClient, methodName, proxyArgs);
            //do receive
            return methodDesc.getReceiveMethod().invoke(serviceClient);
        } finally {
            this.serviceClientFactory.destroy(serviceClient);
        }
    }

    public <I> I newProxyInstance(Class<I> interfaceClass) {
        return (I) Proxy.newProxyInstance(ClassLoader.getSystemClassLoader(), new Class<?>[]{interfaceClass}, this);
    }

    private static <T extends TServiceClient> Map<String, MethodDesc> getServiceMethodDescMap(Class<T> clazz) throws NoSuchMethodException {
        Map<String, Class<? extends TBase>> argsClasses = new HashMap<>();
        for (Class<?> innerClass : clazz.getDeclaringClass().getClasses()) {
            String name = innerClass.getSimpleName();
            if (TBase.class.isAssignableFrom(innerClass) && name.endsWith("_args")) {
                argsClasses.put(name.substring(0, name.length() - 5), (Class<? extends TBase>) innerClass);
            }
        }

        Map<String, Method> sendMethods = new HashMap<>();
        Map<String, Method> receiveMethods = new HashMap<>();
        for (Method method : clazz.getDeclaredMethods()) {
            String name = method.getName();
            if (name.startsWith("send_")) {
                sendMethods.put(name.substring(5), method);
            } else if (name.startsWith("recv_")) {
                receiveMethods.put(name.substring(5), method);
            }
        }

        Map<String, MethodDesc> proxyMethods = new HashMap<>((int) Math.ceil(argsClasses.size() / 0.75));
        for (Map.Entry<String, Class<? extends TBase>> entry : argsClasses.entrySet()) {
            String name = entry.getKey();
            Method sendMethod = sendMethods.get(name);
            Method receiveMethod = receiveMethods.get(name);
            MethodDesc<? extends TBase> methodDesc = new MethodDesc<>(sendMethod, receiveMethod, entry.getValue());
            proxyMethods.put(name, methodDesc);
        }
        return proxyMethods;
    }

    @Getter
    private static class MethodDesc<T extends TBase> {

        private final Method sendMethod;

        private final Method receiveMethod;

        private final Constructor<T> argsConstructor;

        public MethodDesc(Method sendMethod, Method receiveMethod, Class<T> argsClass) throws NoSuchMethodException {
            this.sendMethod = Objects.requireNonNull(sendMethod);
            this.receiveMethod = Objects.requireNonNull(receiveMethod);
            this.argsConstructor = Objects.requireNonNull(argsClass.getConstructor(sendMethod.getParameterTypes()));
        }
    }
}
