-- ============================================================================
-- 02_bronze_seed.sql
-- Synthetic demo data loaded into Bronze as strings (raw-landing shape).
-- Dataset size: 8 customers, 10 products, 25 orders, 57 line items.
-- Time window: 2026-02-05 .. 2026-04-18 (with "last month" = March 2026).
-- Designed so the "most sold product in March 2026" has a clear winner
--   (Mechanical Keyboard, 16 units delivered in March).
-- ============================================================================

USE orders_bronze;

TRUNCATE TABLE bronze_customers;
TRUNCATE TABLE bronze_products;
TRUNCATE TABLE bronze_orders;
TRUNCATE TABLE bronze_order_items;

-- ---------- customers ----------
INSERT INTO bronze_customers (
  customer_id, first_name, last_name, email, phone, created_at, _ingest_ts
) VALUES
  ('1', 'Alice',  'Johnson', 'alice.j@example.com',      '+1-555-0101', '2025-11-03 09:12:00', current_timestamp()),
  ('2', 'Bob',    'Smith',   'bob.smith@example.com',    '+1-555-0102', '2025-11-14 10:44:00', current_timestamp()),
  ('3', 'Carol',  'Davis',   'carol.d@example.com',      '+1-555-0103', '2025-12-02 14:03:00', current_timestamp()),
  ('4', 'David',  'Chen',    'david.chen@example.com',   '+1-555-0104', '2025-12-21 16:22:00', current_timestamp()),
  ('5', 'Eve',    'Miller',  'eve.m@example.com',        '+1-555-0105', '2026-01-07 08:55:00', current_timestamp()),
  ('6', 'Frank',  'Lee',     'frank.lee@example.com',    '+1-555-0106', '2026-01-19 11:30:00', current_timestamp()),
  ('7', 'Grace',  'Kim',     'grace.kim@example.com',    '+1-555-0107', '2026-02-02 13:17:00', current_timestamp()),
  ('8', 'Henry',  'Patel',   'henry.p@example.com',      '+1-555-0108', '2026-02-15 15:48:00', current_timestamp());

-- ---------- products ----------
INSERT INTO bronze_products (
  product_id, sku, product_name, category, unit_price, created_at, _ingest_ts
) VALUES
  ('1',  'KB-MECH-01',  'Mechanical Keyboard',  'Electronics',  '129.99', '2025-10-01 00:00:00', current_timestamp()),
  ('2',  'MS-WRL-01',   'Wireless Mouse',       'Electronics',  '39.99',  '2025-10-01 00:00:00', current_timestamp()),
  ('3',  'HB-USBC-01',  'USB-C Hub',            'Electronics',  '49.99',  '2025-10-01 00:00:00', current_timestamp()),
  ('4',  'LP-STND-01',  'Laptop Stand',         'Accessories',  '59.99',  '2025-10-01 00:00:00', current_timestamp()),
  ('5',  'DM-MAT-01',   'Desk Mat',             'Accessories',  '24.99',  '2025-10-01 00:00:00', current_timestamp()),
  ('6',  'MG-COF-01',   'Coffee Mug',           'Kitchen',      '14.99',  '2025-10-01 00:00:00', current_timestamp()),
  ('7',  'WB-BTL-01',   'Water Bottle',         'Kitchen',      '19.99',  '2025-10-01 00:00:00', current_timestamp()),
  ('8',  'NB-A5-01',    'Notebook A5',          'Stationery',   '9.99',   '2025-10-01 00:00:00', current_timestamp()),
  ('9',  'PN-SET-01',   'Pen Set',              'Stationery',   '12.99',  '2025-10-01 00:00:00', current_timestamp()),
  ('10', 'MA-DUAL-01',  'Monitor Arm',          'Accessories',  '89.99',  '2025-10-01 00:00:00', current_timestamp());

-- ---------- orders ----------
INSERT INTO bronze_orders (
  order_id, customer_id, order_date, shipping_address, _ingest_ts
) VALUES
  -- February 2026 (5 orders)
  ('1',  '1', '2026-02-05 09:00:00', '12 Oak St, Seattle WA',      current_timestamp()),
  ('2',  '2', '2026-02-10 10:30:00', '44 Maple Ave, Portland OR',  current_timestamp()),
  ('3',  '3', '2026-02-15 14:15:00', '7 Pine Rd, San Jose CA',     current_timestamp()),
  ('4',  '4', '2026-02-20 16:00:00', '91 Elm Dr, Austin TX',       current_timestamp()),
  ('5',  '5', '2026-02-25 11:45:00', '23 Cedar Ln, Denver CO',     current_timestamp()),
  -- March 2026 — the "last month" window (15 orders)
  ('6',  '1', '2026-03-02 08:20:00', '12 Oak St, Seattle WA',      current_timestamp()),
  ('7',  '2', '2026-03-04 12:10:00', '44 Maple Ave, Portland OR',  current_timestamp()),
  ('8',  '3', '2026-03-06 17:30:00', '7 Pine Rd, San Jose CA',     current_timestamp()),
  ('9',  '4', '2026-03-08 09:45:00', '91 Elm Dr, Austin TX',       current_timestamp()),
  ('10', '5', '2026-03-10 14:00:00', '23 Cedar Ln, Denver CO',     current_timestamp()),
  ('11', '6', '2026-03-12 10:00:00', '58 Birch Ct, Boston MA',     current_timestamp()),
  ('12', '7', '2026-03-14 15:25:00', '17 Spruce Way, Chicago IL',  current_timestamp()),
  ('13', '8', '2026-03-16 11:05:00', '88 Ash Blvd, Miami FL',      current_timestamp()),
  ('14', '1', '2026-03-18 13:40:00', '12 Oak St, Seattle WA',      current_timestamp()),
  ('15', '2', '2026-03-20 09:15:00', '44 Maple Ave, Portland OR',  current_timestamp()),
  ('16', '3', '2026-03-22 16:50:00', '7 Pine Rd, San Jose CA',     current_timestamp()),
  ('17', '4', '2026-03-24 10:20:00', '91 Elm Dr, Austin TX',       current_timestamp()),
  ('18', '5', '2026-03-26 14:55:00', '23 Cedar Ln, Denver CO',     current_timestamp()),
  ('19', '6', '2026-03-28 08:30:00', '58 Birch Ct, Boston MA',     current_timestamp()),
  ('20', '7', '2026-03-30 12:00:00', '17 Spruce Way, Chicago IL',  current_timestamp()),
  -- April 2026 (5 orders, some still pending)
  ('21', '1', '2026-04-03 09:30:00', '12 Oak St, Seattle WA',      current_timestamp()),
  ('22', '2', '2026-04-08 11:20:00', '44 Maple Ave, Portland OR',  current_timestamp()),
  ('23', '3', '2026-04-12 15:45:00', '7 Pine Rd, San Jose CA',     current_timestamp()),
  ('24', '4', '2026-04-15 10:10:00', '91 Elm Dr, Austin TX',       current_timestamp()),
  ('25', '5', '2026-04-18 13:55:00', '23 Cedar Ln, Denver CO',     current_timestamp());

-- ---------- order_items ----------
-- Status values: PENDING, SHIPPED, DELIVERED, CANCELLED, RETURNED
INSERT INTO bronze_order_items (
  order_item_id, order_id, product_id, quantity, unit_price,
  line_status, status_updated_at, _ingest_ts
) VALUES
  -- Order 1 (Feb 5, Alice)
  ('1',  '1',  '1',  '1', '129.99', 'DELIVERED', '2026-02-09 15:00:00', current_timestamp()),
  ('2',  '1',  '4',  '1', '59.99',  'DELIVERED', '2026-02-09 15:00:00', current_timestamp()),
  ('3',  '1',  '5',  '2', '24.99',  'DELIVERED', '2026-02-09 15:00:00', current_timestamp()),
  -- Order 2 (Feb 10, Bob)
  ('4',  '2',  '2',  '2', '39.99',  'DELIVERED', '2026-02-14 12:00:00', current_timestamp()),
  ('5',  '2',  '3',  '1', '49.99',  'DELIVERED', '2026-02-14 12:00:00', current_timestamp()),
  -- Order 3 (Feb 15, Carol)
  ('6',  '3',  '6',  '4', '14.99',  'DELIVERED', '2026-02-18 17:00:00', current_timestamp()),
  ('7',  '3',  '7',  '2', '19.99',  'DELIVERED', '2026-02-18 17:00:00', current_timestamp()),
  ('8',  '3',  '8',  '3', '9.99',   'DELIVERED', '2026-02-18 17:00:00', current_timestamp()),
  -- Order 4 (Feb 20, David)
  ('9',  '4',  '10', '1', '89.99',  'DELIVERED', '2026-02-25 10:00:00', current_timestamp()),
  ('10', '4',  '9',  '2', '12.99',  'DELIVERED', '2026-02-25 10:00:00', current_timestamp()),
  -- Order 5 (Feb 25, Eve)
  ('11', '5',  '1',  '1', '129.99', 'DELIVERED', '2026-03-01 14:00:00', current_timestamp()),
  ('12', '5',  '2',  '1', '39.99',  'DELIVERED', '2026-03-01 14:00:00', current_timestamp()),
  -- Order 6 (Mar 2, Alice)  --- MARCH starts ---
  ('13', '6',  '1',  '2', '129.99', 'DELIVERED', '2026-03-06 11:00:00', current_timestamp()),
  ('14', '6',  '2',  '2', '39.99',  'DELIVERED', '2026-03-06 11:00:00', current_timestamp()),
  ('15', '6',  '6',  '1', '14.99',  'DELIVERED', '2026-03-06 11:00:00', current_timestamp()),
  -- Order 7 (Mar 4, Bob)
  ('16', '7',  '1',  '1', '129.99', 'DELIVERED', '2026-03-09 09:30:00', current_timestamp()),
  ('17', '7',  '3',  '1', '49.99',  'DELIVERED', '2026-03-09 09:30:00', current_timestamp()),
  -- Order 8 (Mar 6, Carol)
  ('18', '8',  '1',  '3', '129.99', 'DELIVERED', '2026-03-11 16:00:00', current_timestamp()),
  ('19', '8',  '5',  '1', '24.99',  'DELIVERED', '2026-03-11 16:00:00', current_timestamp()),
  -- Order 9 (Mar 8, David)
  ('20', '9',  '2',  '1', '39.99',  'DELIVERED', '2026-03-13 13:00:00', current_timestamp()),
  ('21', '9',  '4',  '1', '59.99',  'DELIVERED', '2026-03-13 13:00:00', current_timestamp()),
  ('22', '9',  '8',  '2', '9.99',   'DELIVERED', '2026-03-13 13:00:00', current_timestamp()),
  -- Order 10 (Mar 10, Eve)
  ('23', '10', '1',  '2', '129.99', 'DELIVERED', '2026-03-15 10:00:00', current_timestamp()),
  ('24', '10', '7',  '3', '19.99',  'DELIVERED', '2026-03-15 10:00:00', current_timestamp()),
  -- Order 11 (Mar 12, Frank)
  ('25', '11', '1',  '1', '129.99', 'DELIVERED', '2026-03-17 12:30:00', current_timestamp()),
  ('26', '11', '9',  '1', '12.99',  'DELIVERED', '2026-03-17 12:30:00', current_timestamp()),
  -- Order 12 (Mar 14, Grace)
  ('27', '12', '1',  '2', '129.99', 'DELIVERED', '2026-03-19 14:00:00', current_timestamp()),
  ('28', '12', '2',  '2', '39.99',  'DELIVERED', '2026-03-19 14:00:00', current_timestamp()),
  ('29', '12', '10', '1', '89.99',  'DELIVERED', '2026-03-19 14:00:00', current_timestamp()),
  -- Order 13 (Mar 16, Henry)
  ('30', '13', '1',  '1', '129.99', 'DELIVERED', '2026-03-21 11:15:00', current_timestamp()),
  ('31', '13', '6',  '2', '14.99',  'DELIVERED', '2026-03-21 11:15:00', current_timestamp()),
  -- Order 14 (Mar 18, Alice) — includes a cancelled line
  ('32', '14', '3',  '2', '49.99',  'DELIVERED', '2026-03-23 15:45:00', current_timestamp()),
  ('33', '14', '5',  '1', '24.99',  'CANCELLED', '2026-03-20 09:00:00', current_timestamp()),
  -- Order 15 (Mar 20, Bob)
  ('34', '15', '1',  '1', '129.99', 'DELIVERED', '2026-03-25 13:00:00', current_timestamp()),
  ('35', '15', '2',  '3', '39.99',  'DELIVERED', '2026-03-25 13:00:00', current_timestamp()),
  -- Order 16 (Mar 22, Carol)
  ('36', '16', '7',  '2', '19.99',  'DELIVERED', '2026-03-27 10:30:00', current_timestamp()),
  ('37', '16', '8',  '5', '9.99',   'DELIVERED', '2026-03-27 10:30:00', current_timestamp()),
  ('38', '16', '1',  '2', '129.99', 'DELIVERED', '2026-03-27 10:30:00', current_timestamp()),
  -- Order 17 (Mar 24, David) — includes a return
  ('39', '17', '4',  '2', '59.99',  'DELIVERED', '2026-03-29 16:00:00', current_timestamp()),
  ('40', '17', '6',  '1', '14.99',  'RETURNED',  '2026-04-02 14:00:00', current_timestamp()),
  -- Order 18 (Mar 26, Eve)
  ('41', '18', '1',  '1', '129.99', 'DELIVERED', '2026-03-31 12:00:00', current_timestamp()),
  ('42', '18', '2',  '1', '39.99',  'DELIVERED', '2026-03-31 12:00:00', current_timestamp()),
  -- Order 19 (Mar 28, Frank) — shipped but not yet delivered
  ('43', '19', '9',  '3', '12.99',  'DELIVERED', '2026-04-02 11:00:00', current_timestamp()),
  ('44', '19', '1',  '1', '129.99', 'SHIPPED',   '2026-04-01 09:00:00', current_timestamp()),
  -- Order 20 (Mar 30, Grace)
  ('45', '20', '10', '1', '89.99',  'DELIVERED', '2026-04-04 15:30:00', current_timestamp()),
  ('46', '20', '2',  '2', '39.99',  'DELIVERED', '2026-04-04 15:30:00', current_timestamp()),
  ('47', '20', '3',  '1', '49.99',  'DELIVERED', '2026-04-04 15:30:00', current_timestamp()),
  -- Order 21 (Apr 3, Alice)   --- APRIL ---
  ('48', '21', '1',  '1', '129.99', 'DELIVERED', '2026-04-10 14:00:00', current_timestamp()),
  ('49', '21', '5',  '1', '24.99',  'DELIVERED', '2026-04-10 14:00:00', current_timestamp()),
  -- Order 22 (Apr 8, Bob)
  ('50', '22', '2',  '2', '39.99',  'SHIPPED',   '2026-04-15 10:00:00', current_timestamp()),
  ('51', '22', '7',  '1', '19.99',  'SHIPPED',   '2026-04-15 10:00:00', current_timestamp()),
  -- Order 23 (Apr 12, Carol)
  ('52', '23', '1',  '2', '129.99', 'SHIPPED',   '2026-04-18 16:00:00', current_timestamp()),
  ('53', '23', '6',  '3', '14.99',  'PENDING',   '2026-04-12 15:45:00', current_timestamp()),
  -- Order 24 (Apr 15, David)
  ('54', '24', '4',  '1', '59.99',  'PENDING',   '2026-04-15 10:10:00', current_timestamp()),
  ('55', '24', '8',  '2', '9.99',   'PENDING',   '2026-04-15 10:10:00', current_timestamp()),
  -- Order 25 (Apr 18, Eve)
  ('56', '25', '9',  '1', '12.99',  'PENDING',   '2026-04-18 13:55:00', current_timestamp()),
  ('57', '25', '10', '1', '89.99',  'PENDING',   '2026-04-18 13:55:00', current_timestamp());
