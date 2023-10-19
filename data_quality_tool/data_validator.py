# from pyspark.sql import SparkSession
# from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

# # Create a Spark session
# spark = SparkSession.builder.appName("demo").getOrCreate()
# # Read the CSV file into a DataFrame
# df = spark.read.option("header", "true").option("inferSchema", "true") \
#     .csv(r"C:\Users\manib\OneDrive\Desktop\data_quality_tooll_\source_dataset\SalesOrderDetail.csv")

# # Create a Great Expectations SparkDFDataset
# gx_df = SparkDFDataset(df)

# # Create an empty list to store the result dictionaries
# result_dicts = []
# print(result_dicts)
# # Define expectations and accumulate the results
# expectation1 = gx_df.expect_column_values_to_be_unique("SalesOrderID")
# result_dict1 = {
#     "expectation_config": expectation1.expectation_config.to_json_dict(),
#     "success": expectation1.success,
#     "expectation_type":expectation1.expectation_config.get("expectation_type"),
#     "expectation_column":expectation1.expectation_config.get("kwargs").get("column"),
#     "result": expectation1.result,
#     "partial_result": expectation1.result.get("partial_unexpected_list"),
#     "exception_info": expectation1.exception_info
# }
# result_dicts.append(result_dict1)

# expectation2 = gx_df.expect_column_to_exist("UnitPrice")
# result_dict2 = {
#     "expectation_config": expectation2.expectation_config.to_json_dict(),
#     "success": expectation2.success,
#     "expectation_type":expectation2.expectation_config.get("expectation_type"),
#     "expectation_column":expectation2.expectation_config.get("kwargs").get("column"),
#     "result": expectation2.result,
#     "partial_result": expectation2.result.get("partial_unexpected_list"),
#     "exception_info": expectation2.exception_info
# }

# result_dicts.append(result_dict2)

# # Create a single DataFrame to accumulate the results
# result_df = spark.createDataFrame(result_dicts)
# # result_df.write.option("header","true").option("inferSchema","true")\
# #     .csv("gx.csv")

# result_df.show(vertical=True, truncate=False)
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
class GreatExpectations:
    def __init__(self,dataset=None,spark=None):
        self.dataset=dataset
        self.spark=spark
    # @pytest.fixture(autouse=True)
    def read_spark(self):
        df=self.spark.read.option("header","true").option("inferSchema","true").csv(self.dataset)
        return df.show()
    def expect_column_to_exist_(self):
        df=self.read_spark()
        gx_df=SparkDFDataset(df)
        data=gx_df.expect_column_to_exist("SalesOrderID")
        print(data)


