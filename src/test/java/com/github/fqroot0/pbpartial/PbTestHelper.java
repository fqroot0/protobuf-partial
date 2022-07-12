package com.github.fqroot0.pbpartial;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;

import lombok.extern.slf4j.Slf4j;

/**
 * @author fqroot0
 * Created on 2022-07-09
 */

@Slf4j
class PbTestHelper {
    public static AddressBook genAddressBook() {
        AddressBook.Builder addrBookBuilder = AddressBook.newBuilder();

        for (int i = 0; i < 3; i++) {
            String name = "foo" + i;
            Person.Builder personBuilder = Person
                    .newBuilder()
                    .setId(i)
                    .setName(name)
                    .setEmail(name + "@xxx.com");
            Person.PhoneNumber.Builder phoneNumber =
                    Person.PhoneNumber.newBuilder().setNumber("000-000-00" + i);
            phoneNumber.setType(Person.PhoneType.MOBILE);
            personBuilder.addPhones(phoneNumber);
            Person p = personBuilder.build();
            addrBookBuilder.addPeople(p);
        }

        AddressBook addrBook = addrBookBuilder.build();
        return addrBook;

    }
}