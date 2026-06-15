INSERT INTO ratchet_test_order(name) VALUES ('second; still one');
UPDATE ratchet_test_order SET name = 'second''s value' WHERE name = 'second; still one';
