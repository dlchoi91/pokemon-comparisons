import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'cfstackname','region_name','bucket_name', 'database'])
path_root = 's3://' + args['bucket_name'] + '/parquetdata/'

# Create a Glue context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



## @type: DataSource
## @args: [database = "pokemon", table_name = "rawjson", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['database'], table_name = "rawjson", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("type", "string", "type", "string"), ("name", "string", "name", "string"), ("url", "string", "url", "string"), ("abilities", "array", "abilities", "array"), ("base_experience", "int", "base_experience", "int"), ("forms", "array", "forms", "array"), ("game_indices", "array", "game_indices", "array"), ("height", "int", "height", "int"), ("held_items", "array", "held_items", "array"), ("id", "int", "id", "int"), ("is_default", "boolean", "is_default", "boolean"), ("location_area_encounters", "string", "location_area_encounters", "string"), ("moves", "array", "moves", "array"), ("order", "int", "order", "int"), ("species", "struct", "species", "struct"), ("sprites", "struct", "sprites", "struct"), ("stats", "array", "stats", "array"), ("types", "array", "types", "array"), ("weight", "int", "weight", "int")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("type", "string", "type", "string"), ("name", "string", "name", "string"), ("url", "string", "url", "string"), ("abilities", "array", "abilities", "array"), ("base_experience", "int", "base_experience", "int"), ("forms", "array", "forms", "array"), ("game_indices", "array", "game_indices", "array"), ("height", "int", "height", "int"), ("held_items", "array", "held_items", "array"), ("id", "int", "id", "int"), ("is_default", "boolean", "is_default", "boolean"), ("location_area_encounters", "string", "location_area_encounters", "string"), ("moves", "array", "moves", "array"), ("order", "int", "order", "int"), ("species", "struct", "species", "struct"), ("sprites", "struct", "sprites", "struct"), ("stats", "array", "stats", "array"), ("types", "array", "types", "array"), ("weight", "int", "weight", "int")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: Relationalize
## @args: [staging_path = "s3://staging-bucket"]
## @return: relationalize4
## @inputs: [frame = dropnullfields3]
relationalize4 = Relationalize.apply(frame = dropnullfields3, staging_path =args["TempDir"], name="pokemon_main")
## @type: DataSink
## @args: [connection_type = "s3", format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = relationalize4]
for df_name in relationalize4.keys():
    m_df = relationalize4.select(df_name)
    path = path_root + df_name
    datasink5 = glueContext.write_dynamic_frame.from_options(frame = m_df, connection_type = "s3", connection_options = {"path": path}, format = "parquet", transformation_ctx = "datasink4")
job.commit()
