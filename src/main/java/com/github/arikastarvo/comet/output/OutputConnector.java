package com.github.arikastarvo.comet.output;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface OutputConnector {

    public String name();

    @SuppressWarnings("rawtypes")
    public Class<? extends OutputConfiguration> configuration();
}