
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
/* Provider, Brand, Presentation, Color, etc... */
CREATE TABLE "clss" (
    id_clss serial PRIMARY KEY NOT NULL,
    name text,
    name_es text,
    match text,
    key text,
    description text,
    source varchar(255)
);

/* Attribute */
/* Chedraui, Advil, Tabletas, Pfizer, ibuprofeno */
CREATE TABLE "attr" (
    id_attr serial PRIMARY KEY NOT NULL,
    id_clss int REFERENCES clss(id_clss),
    name text,
    key text,
    match text,
    has_value int,
    meta json,
    source text
);

/*Categories*/
CREATE TABLE "category" (
    id_category serial PRIMARY KEY NOT NULL,
    id_parent int REFERENCES category(id_category),
    source character varying(255) REFERENCES source(key),
    name text,
    key text,
    code varchar(255)
);

/* Group: exact same products */
CREATE TABLE "item" (
    item_uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    gtin varchar(14),
    checksum integer,
    name varchar(255),
    description text,
    page_views integer,
    last_modified timestamp
 );

/* productRetailer */
CREATE TABLE "product" (
    product_uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    item_uuid uuid REFERENCES "item" (item_uuid),
    source varchar(255) not null,
    product_id varchar(255) not null,
    name varchar(255),
    gtin varchar(14),
    description text,
    normalized text,
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

/* productImage */
CREATE TABLE "product_image" (
    id_product_image serial PRIMARY KEY,
    product_uuid uuid REFERENCES "product" (product_uuid),
    image text,
    descriptor json,
    last_modified timestamp
);

/* productAttribute */
CREATE TABLE "product_attr" (
    id_product_attr serial PRIMARY KEY NOT NULL,
    id_attr integer REFERENCES attr(id_attr),
    product_uuid uuid REFERENCES product(product_uuid),
    value text,
    precision text,
    last_modified timestamp
);

/* itemAttribute */
/* Normalized attributes */
CREATE TABLE "item_attr" (
    id_item_attr serial PRIMARY KEY NOT NULL,
    id_attr integer REFERENCES attr(id_attr),
    item_uuid uuid REFERENCES item(item_uuid),
    value text,
    precision text,
    last_modified timestamp
);

/* productCategory */
CREATE TABLE "product_category" (
    id_product_category serial PRIMARY KEY NOT NULL,
    id_category int REFERENCES category(id_category),
    product_uuid uuid REFERENCES product(product_uuid),
    last_modified timestamp
);

/* productNormalized */ 

/*
-- Batch created table, no need to define at initial schema.
CREATE TABLE "product_normalized" (
    product_uuid uuid PRIMARY KEY NOT NULL,
    normalized text
);
*/

/* itemVademecumInfo */
/* Batch created table, not need to define initial schema.
CREATE TABLE "item_vademecum_info" (
    item_uuid uuid PRIMARY KEY NOT NULL,
    data json,
    blacklisted bool
);
*/

/* Indexes */
CREATE INDEX ON product (source);
CREATE INDEX ON product (product_id);
CREATE INDEX ON product (product_uuid);
CREATE INDEX ON attr (key);
CREATE INDEX ON attr (id_clss);
CREATE INDEX ON clss (key);
CREATE INDEX ON category (source);