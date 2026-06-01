-- Populate billing_mode with COMMERCIAL for any rows where it is empty or null
UPDATE aggregated_usages
SET billing_mode = 'COMMERCIAL'
WHERE billing_mode IS NULL OR billing_mode = '';