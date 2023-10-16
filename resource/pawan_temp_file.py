import json,os,sys

from logger import *
from spark import *
from utils import *

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

conf_file = openJson(filepath=f"{project_dir}/config/logger_config.json")

log_obj = LoggingConfig(config=conf_file["log_config"])  # Configure the logger
logger = log_obj.configure_logger()

# Create a SparkClass instance
spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)

spark = spark_class.start_spark()  

df=spark_class.data_load(file_format='csv',file_path='source_dataset\SalesOrderDetail.csv')
df.show()


