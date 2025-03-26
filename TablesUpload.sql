/*Carga de datos en la tabla de dimensiones de clientes */

		MERGE INTO dim_clientes AS target
		USING staging_transacciones AS source
		ON target.numero_identificacion = source.numero_identificacion
		WHEN MATCHED THEN
			UPDATE SET target.nombre = source.nombres,
					target.direccion = source.direccion,
					target.telefono = source.telefono,
					target.email = source.email,
					target.reporte_riesgo = source.reporte_riesgo,
					target.monto_riesgo = source.monto_riesgo,
					target.tiempo_mora = source.tiempo_mora
		WHEN NOT MATCHED THEN
			INSERT (tipo_identificacion, numero_identificacion, nombre, direccion, telefono, email, reporte_riesgo, monto_riesgo, tiempo_mora)
			VALUES (source.tipo_identificacion, source.numero_identificacion, source.nombres, source.direccion, source.telefono, source.email, source.reporte_riesgo, source.monto_riesgo, source.tiempo_mora);
/*Carga de datos en la tabla de dimensiones de de productos */			
		MERGE INTO dim_productos_financieros AS target
		USING staging_transacciones AS source
		ON target.numero_cuenta = source.numero_cuenta
		WHEN MATCHED THEN
			UPDATE SET target.tipo_producto = source.tipo_producto
		WHEN NOT MATCHED THEN
			INSERT (numero_cuenta, tipo_producto)
			VALUES (source.numero_cuenta, source.tipo_producto);	
			
/*Carga de datos en la tabla de transacciones */		
		INSERT INTO fact_transacciones (id_cliente, id_producto, tipo_transaccion, monto, ciudad, fecha)
		SELECT 
			c.id_cliente, 
			p.id_producto,  
			s.tipo_transaccion, 
			s.monto, 
			s.ciudad, 
			s.fecha
		FROM staging_transacciones s
		JOIN dim_clientes c ON s.numero_identificacion = c.numero_identificacion
		JOIN dim_productos_financieros p ON s.numero_cuenta = p.numero_cuenta
		JOIN dim_tiempo t ON s.fecha = t.fecha;

