# Contracts

## Item related endpoints

### GET `/item/<item_uuid>?retailer=<retailer_key>`
#### Get item short info by retailer
* **Response:**  
```javascript
{
    "description": "LIPITOR 2 Caja, 15 Tabletas, 80 mg", 
    "gtin": "07501287640925", 
    "images": [
        "https://super.walmart.com.mx//images/product-images/img_small/00750128764092s.jpg", 
        "https://super.walmart.com.mx//images/product-images/img_large/00750128764092L.jpg"
    ], 
    "item_uuid": "1af56b72-9955-4985-861a-69fddc6c0646", 
    "name": "LIPITOR 2 Caja, 15 Tabletas, 80 mg,  ", 
    "retailer": "Walmart", 
    "retailer_key": "walmart", 
    "url": "https://super.walmart.com.mx/Medicamentos-de-Patente/Lipitor-80-mg-tabletas-15-pzas/00750128764092?storeId=0000003845"
}
```

### GET `/item/catalogue?&p=<page>&ipp=<items_per_page>`
#### Get item catalogue by retailer
**Response:**
```javascript
{
  "items": [
    {
      "additional": [], 
      "attributes": [
        {
          "attr_key": [
            "tablet"
          ], 
          "attr_name": [
            "tabletas"
          ], 
          "class_name": null, 
          "value": "92"
        }
      ], 
      "brand": {
        "key": "af valdecasas", 
        "name": "A.F. VALDECASAS"
      }, 
      "categories": [
        [ "Medicamentos"]
      ], 
      "date": "2017-08-02 00:00:00", 
      "descriptions": [
        "acido folico 5 mg oral 92 tab"
      ], 
      "gtin": "07501446000348", 
      "images": null, 
      "ingredients": [
        "['acido folico']", 
        "['PTERIDINE', 'PYRIDINE', 'FOLIC ACID']"
      ], 
      "item_uuid": "3321f4fd-fd0e-42dc-8dcf-c0fa8effff5b", 
      "name": "A.F. VALDECASAS Frasco(s),92 Tabletas,5 mg", 
      "names": [
        "valdecas 92 tab caja", 
        "A.F. VALDECASAS ACIDO FOLICO  5 MG. C/92 TAB"
      ], 
      "provider": {
        "key": "valdecasas sa laboratorios", 
        "name": "VALDECASAS, S.A., LABORATORIOS"
      }, 
      "retailers": [
        "san_pablo", 
        "f_ahorro"
      ]
    },
    //...
  ],
  "missing" : "<true | false>"
}
```

### GET `/item/categories`
#### Get list of item categories
**Response:**
```javascript
{
    "categories": [
        {
            "code": null, 
            "id_category": 9646, 
            "id_parent": null, 
            "key": "equipo y botiquin cuidado de la salud", 
            "name": "Equipo y Botiqu\u00edn / Cuidado de la salud", 
            "retailer": "byprice", 
            "retailer_reference": null
        },
        //...
    ]
}
```

### GET `/item/info/<info>`
#### Get item detailed information
**Response:**
```javascript
{
    "additional": [], 
    "attributes": [
        {
        "attr_key": [
            "mg", 
            "tablet", 
            "tablet"
        ], 
        "attr_name": [
            "mg", 
            "tabletas", 
            "tabletas"
        ], 
        "class_name": null, 
        "value": "275"
        }
    ], 
    "brand": {
        "key": "flanax", 
        "name": "FLANAX"
    }, 
    "categories": [
        [
        "Medicamentos"
        ], 
        [
        "Farmacia", 
        "Analgésicos y sueros"
        ], 
    ], 
    "descriptions": [
        " flanax tab recubiertas 275 mg 20 uas ", 
        "flanax tab recubiertas 275 mg 20 uas", 
    ], 
    "images": [
        "https://super.walmart.com.mx//images/product-images/img_small/00750100849735s.jpg", 
        "http://www.superama.com.mx/Content/images/products/img_large/0750100849735L.jpg", 
    ], 
    "ingredients": [
        "['PROPIONIC ACID AND DERIVATIVES', 'PROPIOPHENONE', 'NAPROXEN']"
    ], 
    "names": [
        " flanax tab recubiertas 275 mg 20 uas ", 
        "flanax 275 mg 20 tab", 
    ], 
    "provider": {
        "key": "bayer de mexico", 
        "name": "BAYER DE MEXICO"
    }, 
    "retailers": [
        "walmart", 
        "superama", 
    ]
}
```

### GET `/item/get_uuid?item_id=<external_id>&retailer=<retailer_key>`
#### Get item uuid given its extenal id and retailer
**Response:**  
```javascript
{
    "description": "SIMILESS CAFE", 
    "gtin": null, 
    "images": null, 
    "item_uuid": "6f54d70f-f703-405e-a0ea-7b95041c9563", 
    "last_modified": "Wed, 23 Aug 2017 00:00:00 GMT", 
    "name": "SIMILESS CAFE"
}
```

### POST `/item/get_uuids`
#### Get list of item uuids given a list of gtins
**Request:**  
```javascript
{
    "gtins": [
        "07501075718188", 
        "07501064550690", 
        "07501089804334", 
        "07502209852273"
    ]
}
```
**Response:**
```javascript
[
    {
        "gtin": "07501075718188",
        "item_uuid": "8b4b9265-cc59-4a1d-b3a1-9d319d1dc584"
    },
    {
        "gtin": "07501064550690",
        "item_uuid": "22cbc81b-25aa-4d61-ae4e-62c9790807a8"
    },
    {
        "gtin": "07501089804334",
        "item_uuid": "7188cae6-e1c7-4485-a9eb-4f3a72702cd4"
    },
    {
        "gtin": "07502209852273",
        "item_uuid": "c6997052-95dc-4949-8fa9-fc7dc9f52ca4"
    }
]
```

### GET `/item/retailer?retailer=<retailer>&icategories=<comma_separated_ids>`
#### Get list of items owned by retailer  
**Response:**  
```javascript
[
  {
    "gtin": "00076808003918", 
    "item_uuid": "9db09eb0-91ea-4aa1-9d3b-8d5b9d894c62", 
    "name": "sopa de codo barilla sin gluten 340 g "
  }, 
  {
    "gtin": "00800469015894", 
    "item_uuid": "08cdcbaf-0101-440f-aab3-533e042afdc7", 
    "name": "taglierini la molisana n\u00b0202 al huevo 250 g "
  }, 
  {
    "gtin": "00029243000271", 
    "item_uuid": "239cc1c3-b9bf-48fd-b3ad-02a49e4d8171", 
    "name": "codito la moderna golden harvest 454 g "
  },
  //...
]
```

### POST `/item/filtered`
#### Get list of items given a set of filters
**Request:**
```javascript
[		
    { "category" : "9406" },
    { "retailer" : "walmart" },
    { "retailer" : "ims" },
    { "item" : "67e8bc34-2e0d-460b-8ed0-72710b19f1b6" },
    { "item" : "08cdcbaf-0101-440f-aab3-533e042afdc7" },
    //...
]
```
**Response**
```javascript
[
    {
        "gtin": "07501299300022",
        "item_uuid": "67e8bc34-2e0d-460b-8ed0-72710b19f1b6",
        "name": "ACLORAL 1 Caja,10 Tabletas,300 mg"
    },
    //...
]
```

## Ingredient related endpoints

### GET `/ingredient`
#### Get item short info by retailer
**Response**
```javascript
[
    {
        "id_ingredient": 609, 
        "name": "acido ibandronico"
    }, 
    {
        "id_ingredient": 744, 
        "name": "acido mefenamico"
    }
    //...
]
```

## Categories related endpoints

### GET `/category?retailer=<retailer_key:opt>`
#### Get list of categories of given retailer
**Response**
```javascript
[
    {
        "id_ingredient": 609, 
        "name": "acido ibandronico"
    }, 
    {
        "id_ingredient": 744, 
        "name": "acido mefenamico"
    }
    //...
]
```

### GET `/category/ims?type=<atc|ch>&levels=<1 to 4 comma separated>&nested=<1|0>`
#### Get list of IMS categories
**Response**
```javascript
[
    {
        "code": "A", 
        "id_category": 8533, 
        "id_parent": 9655, 
        "key": "aparato digesty metabol", 
        "name": "APARATO DIGEST.Y METABOL", 
        "nested": [
            {
                "code": "A01", 
                "id_category": 8534, 
                "id_parent": 8533, 
                "key": "estomatologicos", 
                "name": "ESTOMATOLOGICOS", 
                "nested": [
                {
                    "code": "A01A", 
                    "id_category": 8535, 
                    "id_parent": 8534, 
                    "key": "estomatologicos", 
                    "name": "ESTOMATOLOGICOS", 
                    "nested": [
                    {
                        "code": "A01A0", 
                        "id_category": 8536, 
                        "id_parent": 8535, 
                        "key": "estomatologicos", 
                        "name": "ESTOMATOLOGICOS", 
                        "nested": []
                    }
                    ]
                }, 
                {
                    "code": "A01B", 
                    "id_category": 8537, 
                    "id_parent": 8534, 
                    "key": "antifungicos bucales", 
                    "name": "ANTIFUNGICOS BUCALES", 
                    "nested": [
                    {
                        "code": "A01B0", 
                        "id_category": 8538, 
                        "id_parent": 8537, 
                        "key": "antifungicos bucales", 
                        "name": "ANTIFUNGICOS BUCALES", 
                        "nested": []
                    }
                    ]
                }
                ]
            },
        ]
    },
    //...
]
```

## Brands related endpoints

### GET `/brand?retailer=<key:opt>&p=<page:opt>&ipp=<num_items:opt>`
#### Get list of brands
**Response**
```javascript
[
  {
    "brand_uuid": "e147e40e-8a9d-47e0-90e8-456dceb00151", 
    "name": "100% NATURAL"
  }, 
  //...
]
```

## Providers related endpoints

### GET `/provider?retailer=<key:opt>&p=<page:opt>&ipp=<num_items:opt>`
#### Get list of brands
**Response**
```javascript
[
  {
    "provider_uuid": "9d8123b3-d5f4-4400-91ec-0930ee7e60af", 
    "name": "AEROMEDIC"
  }, 
  //...
]
```