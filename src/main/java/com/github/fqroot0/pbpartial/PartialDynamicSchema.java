package com.github.fqroot0.pbpartial;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor.Syntax;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema.Builder;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.EnumDefinition;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.MessageDefinition;

import java.util.Collection;
import java.util.stream.Collectors;

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

        final MessageDefinition dynamicSchema = toDynamicMessage(descriptor, field);
        schemaBuilder.addMessageDefinition(dynamicSchema);

        try {
            return schemaBuilder.build();
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static MessageDefinition toDynamicMessage(Descriptors.Descriptor desc, Field field) {
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
                    msgBuilder.addField(label, type, subFieldDesc.getName(), subFieldDesc.getNumber(),
                            String.valueOf(subFieldDesc.getDefaultValue()), null, null);
                    break;
                case ENUM:
                    if (!msgBuilder.containsEnum(subFieldDesc.getEnumType().getName())) {
                        EnumDescriptor enumType = subFieldDesc.getEnumType();
                        EnumDefinition.Builder enumBuilder = EnumDefinition.newBuilder(enumType.getName());
                        enumType.getValues().forEach(enumValueDesc -> {
                            enumBuilder.addValue(enumValueDesc.getName(), enumValueDesc.getNumber());
                        });
                        msgBuilder.addEnumDefinition(enumBuilder.build());
                    }
                    msgBuilder.addField(label, subFieldDesc.getEnumType().getName(),
                            subFieldName, subFieldDesc.getNumber(),
                            null, null, null);
                    break;
                case MESSAGE:
                    if (!msgBuilder.containsMessage(subFieldDesc.getMessageType().getName())) {
                        if (subFieldDesc.isMapField()) {
                            label = "repeated";
                            final FieldDescriptor keyFieldDesc = subFieldDesc.getMessageType().findFieldByName(PB_MAP_KEY_NAME);
                            final FieldDescriptor valueFieldDesc = subFieldDesc.getMessageType().findFieldByName(PB_MAP_VALUE_NAME);
                            MessageDefinition.Builder mapMsgBuilder = MessageDefinition.newBuilder(subFieldDesc.getMessageType().getName());
                            mapMsgBuilder.setMapEntry(true);
                            mapMsgBuilder.addField(null, keyFieldDesc.getMessageType().getName(), PB_MAP_KEY_NAME, 1, null, null, null);
                            mapMsgBuilder.addField(null, valueFieldDesc.getMessageType().getName(), PB_MAP_VALUE_NAME, 2, null, null, null);
                            msgBuilder.addMessageDefinition(mapMsgBuilder.build());
                        } else {
                            MessageDefinition msgD = toDynamicMessage(subFieldDesc.getMessageType(), subField);
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
}
