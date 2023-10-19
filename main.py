
from resource.logger import *
from resource.spark import *
from resource.utils import *
from data_quality_tool.data_validator import GreatExpectations as GX
if __name__== "__main__":
    if len(sys.argv) != 2:
        print("Please pass two argument with spark submit")
        sys.exit(1)
    project_dir = os.path.dirname(os.path.abspath(__file__))
    filepath=f"{project_dir}/config/logger_config.json"
    conf_file = openJson(filepath=f"{project_dir}/config/logger_config.json")
    log_obj = LoggingConfig(config=conf_file["log_config"])  # Configure the logger
    logger = log_obj.configure_logger()
    spark_class = SparkClass(config=conf_file["spark_conf"], logger_obj=logger)
    spark = spark_class.start_spark()
    config_path = sys.argv[1]
    user_config=openJson(filepath=config_path)
    dataset=user_config.get("source_location")
    expectation=user_config.get("test_cases")[0].get("expectations")
    print(expectation)
    obj=GX(dataset,spark)
    if expectation=="expect_column_to_exist":
        obj.expect_column_to_exist_()


  






