SELECT 
	country,
	COUNT(*) AS tx_count,
	AVG(amount) AS avg_amount
FROM payments
GROUP BY country
ORDER BY tx_count DESC;