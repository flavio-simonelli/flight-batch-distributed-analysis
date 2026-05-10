package it.uniroma2.sae.util;

import java.io.Serializable;
import java.util.Comparator;

@FunctionalInterface
public interface SerializableComparator<T> extends Comparator<T>, Serializable {}
