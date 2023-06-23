package com.tan.paimon.mkt.infusion.pump.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Documented
@Target(ElementType.FIELD)
@Retention(RUNTIME)
public @interface TransientSink {

}
