IF OBJECT_ID('migration_example', 'U') IS NULL
BEGIN
  CREATE TABLE migration_example (value VARCHAR(40));
  INSERT INTO migration_example (value) VALUES ('a; b');
END;
INSERT INTO migration_example (value) VALUES ('after block');
