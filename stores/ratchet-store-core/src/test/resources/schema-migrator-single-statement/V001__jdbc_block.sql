-- ratchet:single-statement
BEGIN
  EXECUTE IMMEDIATE 'SELECT 1 FROM dual';
  NULL;
END;
