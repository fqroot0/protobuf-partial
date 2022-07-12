package com.github.fqroot0.pbpartial;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.example.tutorial.protos.AddressBook;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class PartialDynamicSchemaTest {
    @Test
    void test() throws InvalidProtocolBufferException {
        final Parser<DynamicMessage> partialParser = PartialDynamicSchema.getPartialParser(AddressBook.getDescriptor(),
                Arrays.asList(
                        "people.name"
                )
        );

        final AddressBook addressBook = PbTestHelper.genAddressBook();
        final byte[] bytes = addressBook.toByteArray();
        final DynamicMessage dynamicMessage = partialParser.parseFrom(bytes);
        log.info("{}", dynamicMessage);

    }

}