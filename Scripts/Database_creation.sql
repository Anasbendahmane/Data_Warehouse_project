use master;
GO


-- Drop and recreate the database
IF EXISTS (SELECT 1 from sys.databases Where name ='datawarehousing')
BEGIN 
  ALTER DATABASE datawarehousing SET SINGLE_USER with ROLLBACK IMMEDIATE ;-- Locks the datawarehousing database for exclusive admin access, immediately forcing the disconnection (ROLLBACK IMMEDIATE) of all other users to prevent blocking the maintenance operation.
  DROP DATABASE datawarehousing.
END;
GO

  
--- Create the database fot the project datawarehousing
Create database Datawarehousing;
GO
---switch to the datawarehousing database

use Datawarehousing;
GO
--Create schemas for each layer in our project

-- Bronze schema
CREATE SCHEMA bronze;
GO
--silver schema

create schema silver;
GO

-- Gold schema
create schema gold;
GO
