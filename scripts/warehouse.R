# DATA WAREHOUSE LOADER

library(DBI)
library(RSQLite)
library(readr)

# -------------------------------
# 1. Load Processed Tables
# -------------------------------

dim_vehicle <- read_csv("data/dim_vehicle.csv")
dim_time <- read_csv("data/dim_time.csv")
dim_location <- read_csv("data/dim_location.csv")
dim_event <- read_csv("data/dim_event.csv")
fact_vehicle_events <- read_csv("data/fact_vehicle_events.csv")

# -------------------------------
# 2. Connect to Warehouse
# -------------------------------

con <- dbConnect(RSQLite::SQLite(), "warehouse/vehicle_dw.sqlite")

# -------------------------------
# 3. Write Tables to Warehouse
# -------------------------------

dbWriteTable(con, "dim_vehicle", dim_vehicle, overwrite = TRUE)
dbWriteTable(con, "dim_time", dim_time, overwrite = TRUE)
dbWriteTable(con, "dim_location", dim_location, overwrite = TRUE)
dbWriteTable(con, "dim_event", dim_event, overwrite = TRUE)
dbWriteTable(con, "fact_vehicle_events", fact_vehicle_events, overwrite = TRUE)

# -------------------------------
# 4. Verify Tables
# -------------------------------

print(dbListTables(con))

dbDisconnect(con)

print("Data Warehouse Successfully Created")