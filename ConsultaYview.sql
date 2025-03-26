/* consulta para identificar los clientes más relevantes */

	SELECT 
		c.id_cliente,
		c.nombre,
		SUM(f.monto) AS total_transacciones,
		COUNT(f.id_transaccion) AS cantidad_transacciones
	FROM fact_transacciones f
	JOIN dim_clientes c ON f.id_cliente = c.id_cliente
	GROUP BY c.id_cliente, c.nombre
	ORDER BY total_transacciones DESC
	LIMIT 10;


/* view para visualización y analisis */

	CREATE OR REPLACE VIEW vw_clientes_rentables AS
		SELECT 
			c.id_cliente,
			c.nombre,
			SUM(f.monto) AS total_transacciones,
			COUNT(f.id_transaccion) AS cantidad_transacciones
		FROM fact_transacciones f
		JOIN dim_clientes c ON f.id_cliente = c.id_cliente
		GROUP BY c.id_cliente, c.nombre;
