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
    "message": "Correctly stored Item!",
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
    "message": "Correctly updated Item!",
    "item_uuid": "sf84sd-68f44gsf86g4-sd8f644g"
}
```

## Delete Item

**Method**:  GET

**Endpoint**: `/item/delete?uuid=<item_uuid | required>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| uuid  | Item UUID | required |

**Response:**

```json
{
    "status": "OK",
    "message": "Item (sf84sd-68f44gsf86g4-sd8f644g) correctly deleted! "
}
```

## Elastic Items

**Method**:  POST

**Endpoint**: `/elastic_items`

**Query Params:**

```json
 {
            "items": "11770ac5-f59d-4362-9a96-ba1595d424f3,1004cc72-7c98-4ac9-9666-a9fd74b26031",
            "type": "item_uuid"
}
```

**Response:**

```json
{
    "status": "OK",
    "message": "Those are the item details :D",
    "items": "<items>"
}
```

## GET CATALOGUE UUIDS

**Method**:  GET

**Endpoint**: `/catalogue_uuids&type=product_uuid`

**Query Params:**

| Param | Description |  Condition   |
| ----- | ----------- | ------------ |
| type  | Item UUID | not required |

**Response:**

```json
{
    "status": "OK",
    "message": "Those are the item and product uuids stored in our DB!",
    "items": "<items>"
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
    "normalized": "LIPITOR 80 mg", // optional
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
            "value": "94B1", // optional
            "attr_key": "medicamentos_de_patente",
            "clss_key": "category",
            "precision": "exact", // optional
            "clss_desc": "Categoría", // optional
        },
        // ...
    ] // optional
}
```

**Response:**

```json
{
    "status": "OK",
    "message": "Product correctly added!",
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

## Reset Product Match

**Method**:  POST

**Endpoint**: `/product/reset`

**Params**:

```json
{
    "puuid": "sf84sd-68f44gsf86g4-sd8f644g", // required
}
```

**Response:**

```json
{
    "status": "OK",
    "message": "Product (sf84sd-68f44gsf86g4-sd8f644g) correctly reset!"
}
```

## Update Product Image

**Method**:  POST

**Endpoint**: `/product/image`

**Params**:

```json
{
    "product_uuid": "sf84sd-68f4sd6f8h4f86g4-sd8f644g", // required
    "image": "super.walmart.com.mx/imagen-fs8dfsag687687g.jpg", // required
    "descriptor": [[2,3,4],[3,4,5]], // optional
}
```

**Response:**

```json
{
    "status": "OK",
    "message": "Product Image correctly updated!"
}
```

## Get Products by Item UUIDs

**Method**:  GET

**Endpoint**: `/product/by/iuuid?keys=<item_uuids | required>&cols=<product_table_fields | optional>&p=<page | optional>&ipp=<items per page | optional>`

**Query Params**:

| Param | Description | Condition |
| ----- | ----------- | --------- |
| keys  | Comma Separated Item UUIDS | required |
| cols  | Comma separated Product fields | optional |
| p | Page | optional, default=1 |
| ipp | Items per page | optional, default=5 |

*Note*: Allowed **cols** are: `item_uuid`, `description`, `normalized`, `gtin`, `raw_product`, `raw_html`, `categories`, `ingredients`, `brand`, `provider`, `url`, `images`, `prod_images`, `prod_attrs` and `prod_categs`.

<a name="get_resp"></a>  **Response:**

```json
{
    "status": "OK",
    "products": [{
        "product_uuid": "sf84sd-68f4sd6f8h4f86g4-sd8f644g",
        "source": "walmart",
        "product_id": "0001245795",
        "item_uuid": "sf84sd-68f44gsf86g4-sd8f644g",
        "name": "LIPITOR 80 mg",
        "last_modified": "2018-03-31",
        "normalized": "lipitor 80 miligramos", // optional
        "description": "2 Caja, 15 Tabletas", // optional
        "gtin": "07501234569781", // optional
        "raw_html": "<body>Lipitor <div>...</div></body>", // optional
        "categories": "Farmacia, Medicina",  // optional
        "ingredients": "Atorvastatina",  // optional
        "brand": "Lipitor", //optional
        "provider": "Pfizer", // optional
        "url": "http://www.walmart.com.mx/LIPITOR%2080%20mg", // optional,
        "images": [
            "http://www.walmart.com.mx/LIPITOR%2080%20mg_LARGE.jpg", "http://www.walmart.com.mx/LIPITOR%2080%20mg_SMAL.jpg"
        ], // optional
        "prod_images": [
            {
                "id_p_image": 45596,
                "image": "Medicamentos de Patente",
                "descriptor": [[0,2,3,1,4,5], [3,4,6,7,7]],
                "last_mod": "2018-01-03"
            }, // ...
        ], // optional
        "prod_attrs": [
            {
                "id_p_attr": 485,
                "value": 80,
                "attr": "Miligramos",
                "clss": "Presentación",
                "last_mod": "2018-01-03"
            }, // ...
        ], // optional
        "prod_categs": [
            {
                "id_p_cat": 75,
                "code": "SD20",
                "cat": "Medicamentos",
                "last_mod": "2018-01-03"
            }, // ...
        ] // optional
    },
    // {}, ...
    ]
}
```

## Get Products by Product UUIDs

**Method**:  GET

**Endpoint**: `/product/by/puuid?keys=<product_uuids | required>&cols=<product_table_fields | optional>&p=<page | optional>&ipp=<items per page | optional>`

**Query Params**:

| Param | Description | Condition |
| ----- | ----------- | --------- |
| keys  | Comma Separated Product UUIDs | required |
| cols  | Comma separated Product fields | optional |
| p | Page | optional, default=1 |
| ipp | Items per page | optional, default=5 |
| orderby | Order by | optional, default=None |

*Note*: Optional **cols** are: `item_uuid`,`description`, `normalized`, `gtin`, `raw_product`, `raw_html`, `categories`, `ingredients`, `brand`, `provider`, `url`, `images`, `last_modified`, `prod_images`, `prod_attrs` and `prod_categs`. If keys is set to `''` it will loop over all **catalogue.product** table.

**Response:**

Same as previous [endpoint](#get_resp).

## Get Products by Source

**Method**:  GET

**Endpoint**: `/product/by/source?keys=<source_key | required>&cols=<product_table_fields | optional>&p=<page | optional>&ipp=<items per page | optional>`

**Query Params**:

| Param | Description | Condition |
| ----- | ----------- | --------- |
| keys  | Comma Separated Source Keys | required |
| cols  | Comma separated Product fields | optional |
| p | Page | optional, default=1 |
| ipp | Items per page | optional, default=5 |

*Note*: Allowed **cols** are: `item_uuid`,`description`, `normalized`, `gtin`, `raw_product`, `raw_html`, `categories`, `ingredients`, `brand`, `provider`, `url`, `images`, `last_modified`, `prod_images`, `prod_attrs` and `prod_categs`.

**Response:**

Same as previous [endpoint](#get_resp).

## Get Products by Attr

**Method**:  GET

**Endpoint**: `/product/by/attr?keys=<attr_key | required>&vals=<values | optional>&rets=<retailer_keys | optional>&cols=<product_table_fields | optional>&p=<page | optional>&ipp=<items per page | optional>`

**Query Params**:

| Param | Description | Condition |
| ----- | ----------- | --------- |
| keys  | Comma Separated Attr Keys | required |
| vals  | Comma Separated Attr Values | optional, default="" |
| rets  | Comma Separated Retailer keys | optional |
| cols  | Product fields to retrieve | optional |
| p | Page | optional, default=1 |
| ipp | Items per page | optional, default=5 |

*Note*: Allowed **cols** are: `item_uuid`,`description`, `normalized`, `gtin`, `raw_product`, `raw_html`, `categories`, `ingredients`, `brand`, `provider`, `url`, `images`, `last_modified`, `prod_images`, `prod_attrs` and `prod_categs`.

**Response:**

Same as previous [endpoint](#get_resp).

## Delete Product

**Method**:  GET

**Endpoint**: `/product/delete?uuid=<item_uuid | required>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| uuid  | Product UUID | required |

**Response:**

```json
{
    "status": "OK",
    "message": "Product correctly deleted! (sf84sd-68f44gsf86g4-sd8f644g)"
}
```

## Delete Product Attr

**Method**:  GET

**Endpoint**: `/product/delete/attr?uuid=<product_uuid | required>&id=<id_prod_attr | required>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| uuid  | Product UUID | required |
| id  | Product Attr ID | required |


**Response:**

```json
{
    "status": "OK",
    "message": "Product Attr correctly deleted!"
}
```

## Delete Product Image

**Method**:  GET

**Endpoint**: `/product/delete/image?uuid=<product_uuid | required>&id=<id_prod_image | required>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| uuid  | Product UUID | required |
| id  | Product Image ID | required |

**Response:**

```json
{
    "status": "OK",
    "message": "Product Image correctly deleted!"
}
```

## Upload Product normalized

**Method**: POST

**Endpoint**: `/product/normalized`

**Request Headers**:

```json
{
    "content-type": "multipart/form-data;"
}
```

**Request Params**:

```
filename=normalized.csv
```

------

# Source

## Get All Sources

**Method**:  GET

**Endpoint**: `/source/?cols=<source_table_fields | optional>`

**Query Params**:

| Param | Description | Condition |
| ----- | ----------- | --------- |
| cols  | Comma Separated Source fields | optional |

*Note*: Optional **cols** are: `logo`,`type`, `hierarchy` and `retailer`.

**Response:**

```json
[
    {
        "key": "farmasmart",
        "name": "Farmasmart",
        "logo":  "farmasmart.png", // optional
        "type": "retailer", // optional
        "retailer": 1 // optional
    },
    {
        "key": "farmacias_similares",
        "name": "Farmacias Similares",
        // ...
    },
    //...
]
```

------

# Errors

- **70001** : "Missing required key params"
- **70002** : "Issues saving in DB!"
- **70003** : "Issues fetching elements in DB"
- **70004** : "Could not apply transaction in DB"
- **70005** : "Wrong DataType to save {table}!"
- **70006** : "Cannot update, {value} not in DB!"
- **70007** : "Missing file, add attachement!"
- **70008** : "Not existing elements in DB!"