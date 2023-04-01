import xarray as xr
import pandas as pd
chunk_size = 1000
# Open the netCDF4 file in chunks using xarray
ds = xr.open_dataset("tx_ens_mean_0.25deg_reg_2011-2022_v26.0e.nc", chunks={"time": chunk_size})

# Loop through each chunk and convert it to a pandas dataframe and save it to a parquet file
for i, chunk in enumerate(ds.chunk({"time": chunk_size}).to_array()):
    print(f"Processing {i}")
    df = chunk.to_dataframe().reset_index()
    file = f"tx_ens_mean_0.25deg_reg_v26.0e_{i}.parquet"
    print(f"\tConverting to parquet")
    df.to_parquet(file, engine="pyarrow")
    print(f"\tSaved {file}")
