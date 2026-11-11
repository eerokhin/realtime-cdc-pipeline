CREATE TABLE customers (
    customer_id BIGSERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT REFERENCES customers(customer_id),
    status VARCHAR(30),
    total_amount NUMERIC(12, 2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Полная старая запись для UPDATE и DELETE в CDC
ALTER TABLE orders REPLICA IDENTITY FULL;

-- Начальные данные
INSERT INTO customers (first_name, email)
VALUES
    ('Ivan', 'ivan@example.com'),
    ('Petr', 'petr@example.com'),
    ('Anna', 'anna@example.com');

INSERT INTO orders (customer_id, status, total_amount)
VALUES
    (1, 'NEW', 1500.00),
    (2, 'PAID', 2500.00),
    (3, 'NEW', 3200.00);
