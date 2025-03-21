		import sys
		from awsglue.context import GlueContext
		from awsglue.transforms import *
		from pyspark.context import SparkContext
		from awsglue.job import Job
		from pyspark.sql.functions import col, trim, upper, to_date, regexp_replace
		
		#  Configurar contexto de AWS Glue
		sc = SparkContext()
		glueContext = GlueContext(sc)
		spark = glueContext.spark_session
		job = Job(glueContext)
		job.init("ETL_Transacciones_A_Redshift", sys.argv)
		
		#  Ruta del archivo en S3
		s3_path = "s3://data-lake-transacciones/datos_transacciones.csv"
		
		#  Leer datos desde S3
		data_source = glueContext.create_dynamic_frame.from_options(
			format_options={"withHeader": True, "separator": ","},
			connection_type="s3",
			format="csv",
			connection_options={"paths": [s3_path]},
			transformation_ctx="data_source"
		)
		
		#  Convertir DynamicFrame a DataFrame para usar funciones de PySpark
		df = data_source.toDF()
		
		#  Limpieza y transformación de datos
		df_cleaned = (
			df
			#  Limpiar espacios en blanco en todas las columnas
			.select([trim(col(c)).alias(c) for c in df.columns])
		
			#  Convertir tipo_transaccion y tipo_producto a MAYÚSCULAS
			.withColumn("tipo_transaccion", upper(col("tipo transacción")))
			.withColumn("tipo_producto", upper(col("tipo de producto")))
		
			#  Estandarizar el formato de fecha (detecta formatos y los convierte a YYYY-MM-DD)
			.withColumn("fecha", to_date(col("fecha-hora"), "yyyy-MM-dd HH:mm:ss"))
			.withColumn("fecha_nacimiento", to_date(col("fecha de nacimiento"), "yyyy-MM-dd"))
		
			#  Eliminar caracteres especiales de montos
			.withColumn("monto_transaccion", regexp_replace(col("monto transacción"), "[^0-9.]", "").cast("decimal(15,2)"))
		
			#  Convertir el tiempo en mora a solo el número de días
			.withColumn("tiempo_mora", regexp_replace(col("tiempo en mora del reporte de riesgo"), "[^0-9]", "").cast("int"))
		)
		
		#  Eliminar duplicados basándose en ID de cliente y fecha de transacción
		df_final = df_cleaned.dropDuplicates(["número de identificación", "fecha"])
		
		#  Convertir DataFrame limpio a DynamicFrame
		dynamic_cleaned = glueContext.create_dynamic_frame.from_dataframe(df_final, glueContext)
		
		#  Cargar los datos transformados en Amazon Redshift
		glueContext.write_dynamic_frame.from_options(
			frame=dynamic_cleaned,
			connection_type="redshift",
			connection_options={
				"dbtable": "staging_transacciones",
				"database": "transacciones_dwh",
				"redshiftTmpDir": "s3://data-lake-transacciones/temp/",
				"aws_iam_role": "arn:aws:iam::tu-cuenta:role/RedshiftRole"
			},
			transformation_ctx="data_redshift"
		)
		
		
		
		
		job.commit()
