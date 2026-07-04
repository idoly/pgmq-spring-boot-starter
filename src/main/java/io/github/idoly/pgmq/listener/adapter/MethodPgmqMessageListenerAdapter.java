package io.github.idoly.pgmq.listener.adapter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import io.github.idoly.pgmq.annotation.PgmqHeader;
import io.github.idoly.pgmq.annotation.PgmqHeaders;
import io.github.idoly.pgmq.message.PgmqMessage;
import io.github.idoly.pgmq.template.PgmqTemplate;
import io.github.idoly.pgmq.listener.PgmqAcknowledgment;
import io.github.idoly.pgmq.listener.PgmqMessageListener;

/**
 * Adapts an annotated bean method to the {@link PgmqMessageListener} contract.
 *
 * <p>Resolves method parameters from the message record, payload, headers, and
 * acknowledgement handle before invoking the target method.</p>
 */
public class MethodPgmqMessageListenerAdapter implements PgmqMessageListener {

    /** Template used to convert payload and header arguments. */
    private final PgmqTemplate pgmqTemplate;

    /** Bean instance that owns the listener method. */
    private final Object bean;

    /** Invocable listener method. */
    private final Method method;

    /** Resolvers ordered to match the listener method parameters. */
    private final List<ArgumentResolver> argumentResolvers;

    /**
     * Create an adapter for a bean method.
     *
     * @param pgmqTemplate template used for payload conversion
     * @param bean listener bean
     * @param method listener method
     */
    public MethodPgmqMessageListenerAdapter(PgmqTemplate pgmqTemplate, Object bean, Method method) {
        Assert.notNull(pgmqTemplate, "PgmqTemplate must not be null");
        Assert.notNull(bean, "Listener bean must not be null");
        Assert.notNull(method, "Listener method must not be null");
        this.pgmqTemplate = pgmqTemplate;
        this.bean = bean;
        this.method = method;
        ReflectionUtils.makeAccessible(this.method);
        this.argumentResolvers = createArgumentResolvers(method);
    }

    /**
     * Invoke the target method with arguments resolved from the message context.
     */
    @Override
    public void onMessage(PgmqMessage message, PgmqAcknowledgment acknowledgment) throws Exception {
        try {
            method.invoke(bean, buildArgs(message, acknowledgment));
        } catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();
            if (targetException instanceof Exception exception) {
                throw exception;
            }
            if (targetException instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

    /**
     * Resolve all listener method arguments for one message.
     */
    private Object[] buildArgs(PgmqMessage message, PgmqAcknowledgment acknowledgment) {
        Object[] args = new Object[argumentResolvers.size()];
        for (int i = 0; i < argumentResolvers.size(); i++) {
            args[i] = argumentResolvers.get(i).resolve(message, acknowledgment);
        }
        return args;
    }

    /**
     * Create one argument resolver per listener method parameter.
     *
     * <p>Resolution order is message record, acknowledgement handle, named header,
     * all headers, then payload. Unannotated {@code Map} parameters are also
     * treated as payload parameters; use {@link PgmqHeaders} to receive headers.</p>
     */
    private List<ArgumentResolver> createArgumentResolvers(Method method) {
        Parameter[] parameters = method.getParameters();
        List<ArgumentResolver> resolvers = new ArrayList<>(parameters.length);

        for (Parameter parameter : parameters) {
            Class<?> parameterType = parameter.getType();
            PgmqHeader header = AnnotatedElementUtils.findMergedAnnotation(parameter, PgmqHeader.class);

            if (PgmqMessage.class.isAssignableFrom(parameterType)) {
                resolvers.add((message, acknowledgment) -> message);
            } else if (PgmqAcknowledgment.class.isAssignableFrom(parameterType)) {
                resolvers.add((message, acknowledgment) -> acknowledgment);
            } else if (header != null) {
                Assert.hasText(header.value(), "@PgmqHeader value must not be empty");
                Type targetType = parameter.getParameterizedType();
                resolvers.add((message, acknowledgment) -> convertHeader(message.headers().get(header.value()), targetType));
            } else if (AnnotatedElementUtils.hasAnnotation(parameter, PgmqHeaders.class)) {
                Assert.isTrue(Map.class.isAssignableFrom(parameterType),
                        "@PgmqHeaders parameter must be assignable from Map");
                resolvers.add((message, acknowledgment) -> message.headers());
            } else {
                Type targetType = parameter.getParameterizedType();
                resolvers.add((message, acknowledgment) -> pgmqTemplate.convertMessagePayload(message, targetType));
            }
        }

        return resolvers;
    }

    /**
     * Convert one header value to the listener method parameter type.
     */
    private Object convertHeader(Object value, Type targetType) {
        return pgmqTemplate.convertPayload(value, targetType);
    }

    /**
     * Resolves a listener method argument from the current message context.
     */
    @FunctionalInterface
    private interface ArgumentResolver {
        Object resolve(PgmqMessage message, PgmqAcknowledgment acknowledgment);
    }
}
