package com.microservis.orderservice.service;

import com.microservis.orderservice.dto.OrderLineItemsDto;
import com.microservis.orderservice.dto.OrderRequest;
import com.microservis.orderservice.model.InventoryResponse;
import com.microservis.orderservice.model.Order;
import com.microservis.orderservice.model.OrderLineItems;
import com.microservis.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Transactional
public class OrderService {

    private final OrderRepository orderRepository;

    private final WebClient.Builder webClientBuilder;

    public void placeOrder(OrderRequest orderRequest){
        Order order = new Order();
        order.setOrderNumber(UUID.randomUUID().toString());

        List<OrderLineItems> orderLineItems = orderRequest.getOrderLineItemsDtoList()
                .stream()
                .map(this::mapToDto)
                .toList();

        order.setOrderLineItemsList(orderLineItems);

        List<String> skuCodes = order.getOrderLineItemsList().stream()
                .map(OrderLineItems::getSkuCode)
                .toList();
        // CAll Inventory Service and place order if product is in
        // stock
       InventoryResponse[] inventoryResponsesArray =  webClientBuilder.build().get()
                .uri("http://inventory-service/api/inventory",uriBuilder -> uriBuilder.queryParam("skuCode",skuCodes).build())
                        .retrieve()
                                .bodyToMono(InventoryResponse[].class)
                                        .block();
       boolean allProductsInStock =  Arrays.stream(inventoryResponsesArray)
               .allMatch(inventoryResponse -> inventoryResponse.isInStock());

       if (allProductsInStock){
           orderRepository.save(order);
       }else{
           throw new IllegalArgumentException("Product is not in stock, please try again later");
       }

    }

    /*
    * Genel ModelMapper oluşturuyoruz */
    private OrderLineItems mapToDto(OrderLineItemsDto orderLineItemsDto) {
        OrderLineItems orderLineItems = new OrderLineItems();
        orderLineItems.setPrice(orderLineItemsDto.getPrice());
        orderLineItems.setQuantity(orderLineItemsDto.getQuantity());
        orderLineItems.setSkuCode(orderLineItemsDto.getSkuCode());
        return orderLineItems;
    }
}
