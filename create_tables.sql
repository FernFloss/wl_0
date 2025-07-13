-- create_tables.sql
CREATE TABLE IF NOT EXISTS Delivery (
    name VARCHAR(255) PRIMARY KEY,
    email VARCHAR(255),
    phone VARCHAR(20),
    zip VARCHAR(20),
    city VARCHAR(255),
    address TEXT,
    region VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Items (
    chrt_id VARCHAR(20) PRIMARY KEY,
    price DECIMAL(10, 2),
    rid VARCHAR(30),
    name VARCHAR(255),
    sale INTEGER,
    total_price DECIMAL(10, 2),
    nm_id VARCHAR(20),
    brand VARCHAR(255),
    status INTEGER
);

CREATE TABLE IF NOT EXISTS Payment (
    transaction VARCHAR(255) PRIMARY KEY,
    request_id VARCHAR(10),
    currency VARCHAR(10),
    provider VARCHAR(50),
    amount DECIMAL(10, 2),
    payment_dt TIMESTAMP WITH TIME ZONE,
    bank VARCHAR(255),
    delivery_cost DECIMAL(10, 2),
    goods_total DECIMAL(10, 2),
    custom_fee DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS Orders (
    order_uid VARCHAR(30) PRIMARY KEY,
    entry VARCHAR(30),
    delivery_id VARCHAR(255) constraint fk_delivery_name not null references Delivery(name),
    payment_id VARCHAR(255) not null REFERENCES Payment(transaction),
    track_number VARCHAR(50),
    locale VARCHAR(10),
    internal_signature VARCHAR(255),
    customer_id VARCHAR(30),
    delivery_service VARCHAR(50),
    shardkey VARCHAR(50),
    sm_id VARCHAR(30),
    date_created TIMESTAMP WITH TIME ZONE,
    oof_shard VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Order_contents (
    order_uid VARCHAR(30) constraint fk_Order_id not null references Orders(order_uid),
    chrt_id VARCHAR(10) constraint fk_item_id not null references Items(chrt_id),
    PRIMARY KEY (order_uid, chrt_id)

);

CREATE TABLE IF NOT EXISTS Hash (
    order_id VARCHAR(30) PRIMARY KEY REFERENCES Orders(order_uid)
);