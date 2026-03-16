SELECT
   auth_method,
   COUNT(*) AS tx_count,
   AVG(amount) AS avg_amount
FROM payments
GROUP BY auth_method
ORDER BY tx_count DESC;