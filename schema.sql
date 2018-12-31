CREATE EXTENSION "uuid-ossp";
CREATE extension pgcrypto;
CREATE extension fuzzystrmatch;

/* Source of Data */
CREATE TABLE "source"(
  key character varying(255) PRIMARY KEY NOT NULL,
  name character varying(255),
  logo character varying(255),
  type character varying(255),
  retailer int,
  hierarchy integer
);

/* Class */
/* Provider, Brand, Presentation, Retailer, Color, etc... */
CREATE TABLE "clss" (
    id_clss serial PRIMARY KEY NOT NULL,
    name text,
    key text,
    has_value int,
    has_qty int,
    has_order int,
    has_unit int
);

/* Attribute */
/* Chedraui, Advil, Tabletas, Pfizer, ibuprofeno */
CREATE TABLE "attr" (
    id_attr serial PRIMARY KEY NOT NULL,
    id_clss int REFERENCES clss(id_clss),
    key text,
    value text
);

/*Categories*/
CREATE TABLE "category" (
    id_category serial PRIMARY KEY NOT NULL,
    id_parent int REFERENCES category(id_category),
    source text REFERENCES source(key),
    name text,
    key text,
    code text
);

/* Group: exact same products */
CREATE TABLE "item" (
    item_uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    gtin varchar(14),
    checksum integer,
    name text,
    description text,
    last_modified timestamp
 );

/* productRetailer */
CREATE TABLE "product" (
    product_uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    item_uuid uuid REFERENCES "item" (item_uuid),
    source text not null,
    product_id text not null,
    name text,
    gtin varchar(14),
    description text,
    raw_product json,
    raw_html text,
    categories text,
    ingredients text,
    brand text,
    provider text,
    url text,
    images text,
    last_modified timestamp
);

CREATE TABLE "product_image" (
    id_product_image serial PRIMARY KEY,
    product_uuid uuid REFERENCES "product" (product_uuid),
    descriptor json,
    image text,
    last_modified timestamp
);

/* productRetailerAttribute */
CREATE TABLE "product_attr" (
    id_product_attr serial PRIMARY KEY NOT NULL,
    id_attr integer REFERENCES attr(id_attr),
    product_uuid uuid REFERENCES product(product_uuid),
    source text REFERENCES source(key),
    value text,
    order_ int,
    qty int,
    unit text,
    last_modified timestamp
);

/* itemRetailerAttribute */
CREATE TABLE "item_attr" (
    id_item_attr serial PRIMARY KEY NOT NULL,
    id_attr integer REFERENCES attr(id_attr),
    item_uuid uuid REFERENCES item(item_uuid),
    value text,
    order_ int,
    qty int,
    unit text,
    last_modified timestamp
);


CREATE TABLE "nutriment" (
    id_nutriment serial PRIMARY KEY NOT NULL,
    name text,
    key text
);

CREATE TABLE "product_nutriment" (
    id_product_nutriment serial PRIMARY KEY NOT NULL,
    id_nutriment int REFERENCES nutriment(id_nutriment),
    product_uuid uuid REFERENCES product(product_uuid),
    qty int,
    source text,
    unit text,
    last_modified timestamp
);


/* productRetailerCategory */
CREATE TABLE "product_category" (
    id_product_category serial PRIMARY KEY NOT NULL,
    id_category int REFERENCES category(id_category),
    product_uuid uuid REFERENCES product(product_uuid),
    last_modified timestamp
);

CREATE TABLE "product_normalized" (
    product_uuid uuid REFERENCES product(product_uuid),
    normalized text
);

/* Indexes */
/*
CREATE INDEX ON product (source);
CREATE INDEX ON product (product_id);
CREATE INDEX ON product (item_uuid);
CREATE INDEX ON attr (key);
CREATE INDEX ON attr (id_clss);
CREATE INDEX ON clss (key);
CREATE INDEX ON category (source);
CREATE INDEX ON product_attr (product_uuid)
CREATE INDEX ON product_category (product_uuid);
*/
