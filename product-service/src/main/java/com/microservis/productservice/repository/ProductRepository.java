package com.microservis.productservice.repository;

import com.microservis.productservice.model.Product;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ProductRepository extends MongoRepository<Product,String > {
}
