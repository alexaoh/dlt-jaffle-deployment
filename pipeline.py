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
  
  @dlt.resource(parallelized = False)
  def orders():
      for page in client.paginate("orders", params={"page_size": 10}):
          yield page
          
  @dlt.resource(parallelized = False)
  def products():
      for page in client.paginate("products", params={"page_size": 10}):
          yield page


  return customers, products #, orders # Skip orders for now for simplicity, since deployment is the focus.

# Configure the pipeline with destination and namespace/schema names in duckdb.
pipeline = dlt.pipeline(
    pipeline_name="jaffle_api_opt",
    destination='duckdb',
    dataset_name="jaffle_api_data",
)

# Run the extract.
run_info = pipeline.run(jaffle_api_source())

# Print row counts of all tables in the destination as a dataframe.
print(pipeline.dataset().row_counts().df())
