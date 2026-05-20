package io.github.sbracely.payment.repository;

import io.github.sbracely.payment.domain.Customer;
import org.springframework.data.repository.CrudRepository;

public interface CustomerRepository extends CrudRepository<Customer, Long> {
}

