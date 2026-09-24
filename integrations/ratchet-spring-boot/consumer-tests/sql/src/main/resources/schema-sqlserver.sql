IF OBJECT_ID(N'consumer_record', N'U') IS NULL
  CREATE TABLE consumer_record (id VARCHAR(80) PRIMARY KEY, state VARCHAR(40));
IF OBJECT_ID(N'consumer_uuid_record', N'U') IS NULL
  CREATE TABLE consumer_uuid_record (
    id UNIQUEIDENTIFIER PRIMARY KEY,
    label VARCHAR(80) NOT NULL,
    occurred_at DATETIMEOFFSET(7) NOT NULL
  );
