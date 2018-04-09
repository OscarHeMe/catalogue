# Item Contracts

## Add new Item

**Method**:  POST

**Endpoint**: `/item/add`

**Params**:

```json
{
    "gtin": "07501234569781", // required
    "name": "LIPITOR 80 mg", // required
    "description": "2 Caja, 15 Tabletas", // required
}
```

**Response:**

```json
{
    "status": "OK",
    "message": "Item correctly added! (sf84sd-68f44gsf86g4-sd8f644g)",
    "item_uuid": "sf84sd-68f44gsf86g4-sd8f644g"
}
```

## Modify existing Item

**Method**:  POST

**Endpoint**: `/item/modify`

**Params**:

```json
{
    "item_uuid": "sf84sd-68f44gsf86g4-sd8f644g", // required
    "gtin": "07501234569781", // required
    "name": "LIPITOR 80 mg", // required
    "description": "2 Caja, 15 Tabletas", // required
}
```

**Response:**

```json
{
    "status": "OK",
    "message": "Item correctly modified! (sf84sd-68f44gsf86g4-sd8f644g)",
    "item_uuid": "sf84sd-68f44gsf86g4-sd8f644g"
}
```

## Delete Item

**Method**:  GET

**Endpoint**: `/item/delete?uuid=<item_uuid | required>`

**Response:**

```json
{
    "status": "OK",
    "message": "Item correctly deleted! (sf84sd-68f44gsf86g4-sd8f644g)"
}
```

------

# Product Contracts

## Add new Product

**Method**:  POST

**Endpoint**: `/product/add`

**Params**:

```json
{
    "source": "walmart", // required
    "product_id": "0001245795", // required
    "item_uuid": "sf84sd-68f44gsf86g4-sd8f644g", // optional
    "name": "LIPITOR 80 mg", // required
    "normalized": "LIPITOR 80 mg", // required
    "description": "2 Caja, 15 Tabletas", // required
    "gtin": "07501234569781", // optional
    "raw_html": "<body>Lipitor <div>...</div></body>", // optional
    "categories": "Farmacia, Medicina",  // optional
    "ingredients": "Atorvastatina",  // optional
    "brand": "Lipitor", //optional
    "provider": "Pfizer", // optional
    "url": "http://www.walmart.com.mx/LIPITOR%2080%20mg", // optional,
    "images": [ "http://www.walmart.com.mx/LIPITOR%2080%20mg_LARGE.jpg", "http://www.walmart.com.mx/LIPITOR%2080%20mg_SMAL.jpg"
    ], // optional
    "attributes": [
        {
            "attr_name": "Medicamentos de Patente",
            "clss_name": "Categoría",
            "value": "94B1",
            "attr_key": "medicamentos_de_patente",
            "clss_key": "category"
        },
        // ...
    ] // optional
}
```

**Response:**

```json
{
    "status": "OK",
    "message": "Product correctly added! (sf84sd-68f44gsf86g4-sd8f644g)",
    "product_uuid": "sf84sd-68f44gsf86g4-sd8f644g"
}
```


## Modify existing Product

**Method**:  POST

**Endpoint**: `/product/modify`

**Params**:

```json
{
    "product_uuid": "sf84sd-68f4sd6f8h4f86g4-sd8f644g", // required
    "source": "walmart", // required
    "product_id": "0001245795", // required
    "item_uuid": "sf84sd-68f44gsf86g4-sd8f644g", // optional
    "name": "LIPITOR 80 mg", // required
    "normalized": "LIPITOR 80 mg", // required
    "description": "2 Caja, 15 Tabletas", // required
    "gtin": "07501234569781", // optional
    "raw_html": "<body>Lipitor <div>...</div></body>", // optional
    "categories": "Farmacia, Medicina",  // optional
    "ingredients": "Atorvastatina",  // optional
    "brand": "Lipitor", //optional
    "provider": "Pfizer", // optional
    "url": "http://www.walmart.com.mx/LIPITOR%2080%20mg", // optional,
    "images": [ "http://www.walmart.com.mx/LIPITOR%2080%20mg_LARGE.jpg", "http://www.walmart.com.mx/LIPITOR%2080%20mg_SMAL.jpg"
    ], // optional
    "attributes": [
        {
            "attr_name": "Medicamentos de Patente",
            "clss_name": "Categoría",
            "value": "94B1",
            "attr_key": "medicamentos_de_patente",
            "clss_key": "category"
        },
        // ...
    ] // optional
}
```

**Response:**

```json
{
    "status": "OK",
    "message": "Product correctly updated! (sf84sd-68f4sd6f8g4f86g4-sd8f644g)",
    "product_uuid": "sf84sd-68f4sd6f8g4f86g4-sd8f644g"
}
```