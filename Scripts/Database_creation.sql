use master;
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
