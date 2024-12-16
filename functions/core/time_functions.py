import dask.dataframe as dd  # Use Dask for parallel processing
# from cudf import DataFrame as cudf  # Uncomment for GPU-accelerated processing with cuDF
import cudf
import dask
import dask.dataframe as dd

dask.config.set({"dataframe.backend": "cudf"})
