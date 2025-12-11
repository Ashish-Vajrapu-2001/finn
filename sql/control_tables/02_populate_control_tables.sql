/*
============================================================================
METADATA-DRIVEN CDC PIPELINE
Script: 02_populate_control_tables.sql
Description: Populates control tables based on the CLV Metadata Catalog.
============================================================================
*/

-- 1. Populate Source Systems
INSERT INTO control.source_systems (source_system_id, source_system_name, description, is_active)
VALUES
('SRC-001', 'Azure SQL ERP', 'Orders, Inventory, Finance', 1),
('SRC-002', 'Azure SQL CRM', 'Customer Master, Support', 1),
('SRC-003', 'Azure SQL Marketing', 'Campaign Management', 1);

-- 2. Populate Table Metadata
-- Note: Derived from "2. Entity Catalog" and "3. Tables Metadata" in prompt
INSERT INTO control.table_metadata
(table_id, source_system_id, source_system_name, schema_name, table_name, primary_key_columns, initial_load_completed, is_active)
VALUES
-- CRM System (SRC-002)
('ENT-001', 'SRC-002', 'Azure SQL CRM', 'CRM', 'Customers', 'CUSTOMER_ID', 0, 1),
('ENT-002', 'SRC-002', 'Azure SQL CRM', 'CRM', 'CustomerRegistrationSource', 'REGISTRATION_SOURCE_ID', 0, 1),
('ENT-011', 'SRC-002', 'Azure SQL CRM', 'CRM', 'INCIDENTS', 'INCIDENT_ID', 0, 1),
('ENT-012', 'SRC-002', 'Azure SQL CRM', 'CRM', 'INTERACTIONS', 'INTERACTION_ID', 0, 1),
('ENT-013', 'SRC-002', 'Azure SQL CRM', 'CRM', 'SURVEYS', 'SURVEY_ID', 0, 1),

-- ERP System (SRC-001)
('ENT-003', 'SRC-001', 'Azure SQL ERP', 'ERP', 'OE_ORDER_HEADERS_ALL', 'ORDER_ID', 0, 1),
('ENT-004', 'SRC-001', 'Azure SQL ERP', 'ERP', 'OE_ORDER_LINES_ALL', 'LINE_ID', 0, 1),
('ENT-005', 'SRC-001', 'Azure SQL ERP', 'ERP', 'ADDRESSES', 'ADDRESS_ID', 0, 1),
('ENT-006', 'SRC-001', 'Azure SQL ERP', 'ERP', 'CITY_TIER_MASTER', 'CITY,STATE', 0, 1), -- Composite Key!
('ENT-007', 'SRC-001', 'Azure SQL ERP', 'ERP', 'MTL_SYSTEM_ITEMS_B', 'INVENTORY_ITEM_ID', 0, 1),
('ENT-008', 'SRC-001', 'Azure SQL ERP', 'ERP', 'CATEGORIES', 'CATEGORY_ID', 0, 1),
('ENT-009', 'SRC-001', 'Azure SQL ERP', 'ERP', 'BRANDS', 'BRAND_ID', 0, 1),

-- Marketing System (SRC-003)
('ENT-010', 'SRC-003', 'Azure SQL Marketing', 'MARKETING', 'MARKETING_CAMPAIGNS', 'CAMPAIGN_ID', 0, 1);

-- 3. Populate Dependencies
-- Note: Derived from "4.2 Relationship Cardinality"
-- Format: (Child, Parent) -> Child depends on Parent
INSERT INTO control.load_dependencies (table_id, depends_on_table_id)
VALUES
-- CRM Dependencies
('ENT-002', 'ENT-001'), -- Registration depends on Customer
('ENT-002', 'ENT-010'), -- Registration depends on Campaign
('ENT-011', 'ENT-001'), -- Incidents depends on Customer
('ENT-011', 'ENT-003'), -- Incidents depends on Order Headers
('ENT-012', 'ENT-011'), -- Interactions depends on Incidents
('ENT-012', 'ENT-001'), -- Interactions depends on Customer
('ENT-013', 'ENT-001'), -- Surveys depends on Customer
('ENT-013', 'ENT-003'), -- Surveys depends on Order Headers
('ENT-013', 'ENT-011'), -- Surveys depends on Incidents

-- ERP Dependencies
('ENT-003', 'ENT-001'), -- Order Header depends on Customer (Cross-Source!)
('ENT-003', 'ENT-005'), -- Order Header depends on Address
('ENT-004', 'ENT-003'), -- Order Lines depends on Order Headers
('ENT-004', 'ENT-007'), -- Order Lines depends on Products
('ENT-005', 'ENT-001'), -- Address depends on Customer
('ENT-005', 'ENT-006'), -- Address depends on City Tier
('ENT-007', 'ENT-008'), -- Product depends on Categories
('ENT-007', 'ENT-009'); -- Product depends on Brands
GO
