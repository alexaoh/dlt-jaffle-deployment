import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

@dlt.source
def jaffle_api_source():
  client = RESTClient(
      base_url = "https://jaffle-shop.scalevector.ai/api/v1",
      paginator=HeaderLinkPaginator(),
  )

  @dlt.resource(parallelized = False)
  def customers():
      for page in client.paginate("customers", params={"page_size": 500}):
          yield page
  
  @dlt.resource(parallelized = True)
  def orders():
      for page in client.paginate("orders"):
          yield page
          
  @dlt.resource(parallelized = True)
  def products():
      for page in client.paginate("products"):
          yield page


  return customers#, orders, products

# Configure the pipeline with destination and namespace/schema names in duckdb.
pipeline = dlt.pipeline(
    pipeline_name="jaffle_api_opt",
    destination='duckdb',
    dataset_name="jaffle_api_data",
)

# Run the extract.
extr_info = pipeline.extract(jaffle_api_source())

# Pretty print information after extraction.
print(extr_info.last_trace)

# Run the normalization.
norm_info = pipeline.normalize()

# Pretty print information after normalization.
print(norm_info.last_trace)

# Run the load.
load_info = pipeline.load()

# Pretty print information after loading.
print(load_info.load_trace)
