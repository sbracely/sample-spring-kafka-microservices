package io.github.sbracely.stock.repository;

import io.github.sbracely.stock.domain.Product;
import org.springframework.data.repository.CrudRepository;

public interface ProductRepository extends CrudRepository<Product, Long> {
}

