package com.github.fqroot0.pbpartial;


import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor.Syntax;
import com.google.protobuf.DiscardUnknownFieldsParser;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Parser;

import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema.Builder;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.EnumDefinition;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.MessageDefinition;

/**
 * @author fqroot0
 * Created on 2022-07-10
 */
public class PartialDynamicSchema {
    public static final String PB_MAP_KEY_NAME = "key";
    public static final String PB_MAP_VALUE_NAME = "value";

    public static DynamicSchema toDynamicSchema(Descriptors.Descriptor descriptor, Field field) {
        Syntax syntax = descriptor.getFile().getSyntax();
        Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setSyntax(syntax.name());

        final MessageDefinition messageDef = toMessageDef(descriptor, field);
        schemaBuilder.addMessageDefinition(messageDef);

        try {
            return schemaBuilder.build();
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static MessageDefinition toMessageDef(Descriptors.Descriptor desc, Field field) {
        Collection<Field> subFields = field.getSubFields();
        if (field.getSubFields().size() == 0) {
            subFields = desc.getFields().stream()
                    .map(i -> new Field(i.getName()))
                    .collect(Collectors.toList());
        }

        MessageDefinition.Builder msgBuilder = MessageDefinition.newBuilder(desc.getName());

        subFields.forEach(subField -> {
            String subFieldName = subField.getName();
            FieldDescriptor subFieldDesc = desc.findFieldByName(subFieldName);
            if (subFieldDesc == null) {
                throw new IllegalArgumentException(subFieldName + "not found in " + desc.toProto());
            }

            String label = getLabel(subFieldDesc);
            String type = subFieldDesc.getType().name().toLowerCase();
            switch (subFieldDesc.getJavaType()) {
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case BOOLEAN:
                case STRING:
                case BYTE_STRING:
                    String defaultValue = subFieldDesc.isRepeated() ? null : String.valueOf(subFieldDesc.getDefaultValue());
                    msgBuilder.addField(label, type, subFieldDesc.getName(), subFieldDesc.getNumber(),
                            defaultValue, null, null);
                    break;
                case ENUM:
                    if (!msgBuilder.containsEnum(subFieldDesc.getEnumType().getName())) {
                        EnumDefinition enumDefinition = toEnumDef(subFieldDesc);
                        msgBuilder.addEnumDefinition(enumDefinition);
                    }
                    msgBuilder.addField(label, subFieldDesc.getEnumType().getName(),
                            subFieldName, subFieldDesc.getNumber(),
                            null, null, null);
                    break;
                case MESSAGE:
                    if (!msgBuilder.containsMessage(subFieldDesc.getMessageType().getName())) {
                        if (subFieldDesc.isMapField()) {
                            label = "repeated";
                            MessageDefinition mapDef = toMapDef(subFieldDesc);
                            msgBuilder.addMessageDefinition(mapDef);
                        } else {
                            MessageDefinition msgD = toMessageDef(subFieldDesc.getMessageType(), subField);
                            msgBuilder.addMessageDefinition(msgD);
                        }
                    }
                    msgBuilder.addField(label, subFieldDesc.getMessageType().getName(),
                            subFieldName, subFieldDesc.getNumber(),
                            null, null, null);
                    break;
            }
        });
        return msgBuilder.build();
    }

    private static MessageDefinition toMapDef(FieldDescriptor subFieldDesc) {
        final FieldDescriptor keyFieldDesc = subFieldDesc.getMessageType().findFieldByName(PB_MAP_KEY_NAME);
        final FieldDescriptor valueFieldDesc = subFieldDesc.getMessageType().findFieldByName(PB_MAP_VALUE_NAME);
        MessageDefinition.Builder mapMsgBuilder = MessageDefinition.newBuilder(subFieldDesc.getMessageType().getName());
        mapMsgBuilder.setMapEntry(true);
        String keyType = keyFieldDesc.getType().name().toLowerCase();
        String valueType = valueFieldDesc.getType().name().toLowerCase();
        if (valueFieldDesc.getType() == FieldDescriptor.Type.MESSAGE) {
            valueType = valueFieldDesc.getMessageType().getName();
            final MessageDefinition messageDefinition = toMessageDef(valueFieldDesc.getMessageType(), new Field(valueFieldDesc.getName()));
            mapMsgBuilder.addMessageDefinition(messageDefinition);
        }
        mapMsgBuilder.addField(null, keyType, PB_MAP_KEY_NAME, 1, null, null, null);
        mapMsgBuilder.addField(null, valueType, PB_MAP_VALUE_NAME, 2, null, null, null);
        return mapMsgBuilder.build();
    }

    private static EnumDefinition toEnumDef(FieldDescriptor subFieldDesc) {
        EnumDescriptor enumType = subFieldDesc.getEnumType();
        EnumDefinition.Builder enumBuilder = EnumDefinition.newBuilder(enumType.getName());
        enumType.getValues().forEach(enumValueDesc -> {
            enumBuilder.addValue(enumValueDesc.getName(), enumValueDesc.getNumber());
        });
        return enumBuilder.build();
    }

    private static String getLabel(FieldDescriptor fieldDescriptor) {
        switch (fieldDescriptor.toProto().getLabel()) {
            case LABEL_OPTIONAL:
                return "optional";
            case LABEL_REQUIRED:
                return "required";
            case LABEL_REPEATED:
                return "repeated";
            default:
                throw new IllegalStateException("Unexpected value: " + fieldDescriptor.toProto().getLabel());
        }
    }

    public static Parser<DynamicMessage> getPartialParser(Descriptors.Descriptor descriptor, List<String> fields) {
        final Field field = Field.parse(descriptor.getName(), fields);
        final DynamicSchema dynamicSchema = toDynamicSchema(descriptor, field);
        Descriptors.Descriptor partialDescriptor = dynamicSchema.getMessageDescriptor(descriptor.getName());
        return DiscardUnknownFieldsParser.wrap(
                DynamicMessage.getDefaultInstance(partialDescriptor).getParserForType()
        );
    }
}
