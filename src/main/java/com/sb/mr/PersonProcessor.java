package com.sb.mr;

import org.springframework.batch.item.ItemProcessor;

public class PersonProcessor implements ItemProcessor<Person, Person> {

	public Person process(Person person) throws Exception {

		person.setAge(person.getAge() + 1);
		return person;

	}

}
