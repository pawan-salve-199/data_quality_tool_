from pyspark.sql import SparkSession
import sys

class SparkClass:
    def __init__(self, logger_obj=None, config={},spark_session=None):
        self.app_name = config.get("app_name")
        self.master = config.get('master')
        self.spark_config = config.get("spark_config")
        self.spark = spark_session
        self.logger = logger_obj


    def start_spark(self):
        try:
            self.logger.info("Entering start_spark method.")
            self.spark = self.create_session()
            self.logger.info("Spark Session Initialized.")
            return self.spark
        except Exception as e:
            self.logger.error(f"Error starting Spark session: {str(e)}")
            sys.exit()


    def create_session(self):
        try:
            spark_builder = SparkSession.builder.appName(self.app_name)
            if self.spark_config:
                for key, val in self.spark_config.items():
                    spark_builder = spark_builder.config(key, val)
            spark = spark_builder.getOrCreate()
            self.logger.info("Configured the Spark for the specified dependencies")
            return spark
        except Exception as e:
            self.logger.error(f"Error creating Spark session: {str(e)}")
            sys.exit()


    def data_load(self, file_path=None, file_format="jdbc", jdbc_params={}, options=None):

        try:
            if options is None:
                options={}

            if not isinstance(self.spark, SparkSession):
                raise ValueError("Spark session is not initialized. Please connect first.")
            

            if file_format.upper()=="CSV":
                df = self.spark.read.format(file_format).options(**options).load(file_path)
            else:
                 raise NotImplementedError("Data source not supported.")
            self.logger.info(f"Data read from {file_format}")

            return df
        
        except Exception as e:
            self.logger.error(f"Error while reading data from : {str(e)}")
            sys.exit()


    def write_data(self, df, mode, file_path, file_format="parquet", **options):
        try:
            if not isinstance(self.spark, SparkSession):
                raise ValueError("Spark session is not initialized. Please connect first.")

            if file_format == "jdbc":
                # Implement JDBC write logic here if required
                pass
            else:
                df.write.format(file_format).mode(mode).save(file_path, **options)
                file_name=file_path.split("\\")[-1]
                self.logger.info(f"Data written to file.")
        except Exception as e:
            self.logger.error(f"Error while writing data: {str(e)}")
            sys.exit()
        


if __name__=="__main__":pass

