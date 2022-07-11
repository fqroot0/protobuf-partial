package com.github.fqroot0.pbpartial;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * @author fqroot0
 * Created on 2022-07-10
 */
public class Field {
    private String name;
    private Map<String, Field> subFields;

    public String getName() {
        return name;
    }

    public Field(String name) {
        this.name = name;
        this.subFields = new HashMap<>();
    }

    Field putSubFieldIfAbsent(Field field) {
        return this.subFields.putIfAbsent(field.getName(), field);
    }

    public Collection<Field> getSubFields() {
        return subFields.values();
    }

    public boolean contains(String name) {
        return subFields.containsKey(name);
    }

    public static Field parse(String name, List<String> subfields) {
        Field field = new Field(name);
        subfields.forEach(f->{
            Field cur = field;
            final String[] fieldPath = StringUtils.split(f, ".");
            for(String fieldName: fieldPath) {
                Field newSubField = new Field(fieldName);
                Field old = cur.putSubFieldIfAbsent(newSubField);
                cur = old == null ? newSubField : old;
            }
        });
        return field;
    }

    private Field getSubField(String name)  {
        return this.subFields.get(name);
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", subFields=" + subFields +
                '}';
    }
}
