package com.github.arikastarvo.comet.input;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface InputConnector {

    public String name();

    @SuppressWarnings("rawtypes")
    public Class<? extends InputConfiguration> configuration();
}