USE master;
GO

IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = 'debezium')
    CREATE LOGIN debezium WITH PASSWORD = 'DebeziumPass123!';
GO

GRANT VIEW SERVER STATE TO debezium;
GO

USE [FP-DWH];
GO

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'debezium')
    CREATE USER debezium FOR LOGIN debezium;
GO

ALTER ROLE db_owner ADD MEMBER debezium;
GO

USE [FP-DWH];
EXEC sys.sp_cdc_enable_db;


-----------------------
-- 1) FP-DWH
USE [FP-DWH];
GO


IF DATABASEPROPERTYEX(DB_NAME(), 'IsCdcEnabled') = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT 'CDC enabled on database ' + DB_Name();
END
ELSE
BEGIN
    PRINT 'CDC already enabled on database ' + DB_Name();
END
GO


EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'dim_customer',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'dim_date',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'dim_product',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'dim_supplier',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'fact_sales',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO

PRINT 'CDC enabled for listed tables.';

USE [FP-DWH];

INSERT INTO dbo.dim_customer 
    (customer_id, first_name, last_name, city, country, phone, start_date, is_current)
VALUES
    (999999, 'Test', 'User', 'Cairo', 'Egypt', '000', CONVERT(date, '2025-11-25'), 1);

--try more inserts to see the changes in cdc tables
INSERT INTO dim_customer (customer_id, city)
VALUES (1001, 'Cairo');

INSERT INTO dim_customer (customer_id, city)
VALUES (9999, 'Cairo');

INSERT INTO dim_customer (customer_id, city)
VALUES (909, 'Cairo');
