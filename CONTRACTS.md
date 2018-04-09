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

# Product Contracts

## Add new Product

**Method**:  POST

**Endpoint**: `/product/add`

**Params**:

```json
{
    // still missing elements
    "name": "LIPITOR 80 mg", // required
    "description": "2 Caja, 15 Tabletas", // required
    "gtin": "07501234569781", // optional
}
```

**Response:**

```javascript
{
    "status": "OK",
    "message": "Product correctly added! (sf84sd-68f44gsf86g4-sd8f644g)",
    "product_uuid": "sf84sd-68f44gsf86g4-sd8f644g"
}
```