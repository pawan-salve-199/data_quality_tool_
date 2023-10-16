import json,os,sys
# sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)),"resources"))

from resource.logger import *

from resource.spark import *
from resource.utils import *

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

conf_file = openJson(filepath=f"{project_dir}/config/logger_config.json")

log_obj = LoggingConfig(config=conf_file["log_config"])  # Configure the logger
logger = log_obj.configure_logger()


# Create a SparkClass instance
spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)

spark = spark_class.start_spark()    