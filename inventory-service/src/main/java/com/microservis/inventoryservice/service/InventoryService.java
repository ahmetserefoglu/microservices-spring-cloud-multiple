package com.microservis.inventoryservice.service;

import com.microservis.inventoryservice.dto.InventoryResponse;
import com.microservis.inventoryservice.repository.InventoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class InventoryService
{
    /*
    * Servisin içerisinde repository oluşturulması istenmiyor best practies değil
    * */
    private final InventoryRepository inventoryRepository;
    @Transactional(readOnly = true)
    public List<InventoryResponse> isInStock(List<String> skuCode){
       return inventoryRepository.findBySkuCodeIn(skuCode).stream()
               .map(inventory ->
                   InventoryResponse.builder()
                           .skuCode(inventory.getSkuCode())
                           .isInStock(inventory.getQuantity()>0)
                           .build()
               ).toList();
    }
}
