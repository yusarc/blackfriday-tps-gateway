SELECT
    transaction_id,
    card_id,
    amount,
    country,
    auth_method,
    event_time
FROM payments
WHERE amount >= 200
ORDER BY amount DESC
LIMIT 100;
