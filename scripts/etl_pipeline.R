# ETL PIPELINE
# Extract + Transform IoT sensor data

library(dplyr)
library(lubridate)
library(readr)

# -------------------------------
# 1. Extract Raw Sensor Data
# -------------------------------

raw_data <- read_csv("data/synthetic_data.csv")

# Convert timestamp
raw_data <- raw_data %>%
  mutate(timestamp = ymd_hms(timestamp))

# -------------------------------
# 2. Data Cleaning
# -------------------------------

processed_data <- raw_data %>%
  filter(!is.na(vehicle_id), !is.na(timestamp), !is.na(speed))

# -------------------------------
# 3. Use Existing Event Labels
# -------------------------------

processed_data <- processed_data %>%
  mutate(
    event_type = event_label,
    severity = case_when(
      event_label == "crash" ~ "high",
      event_label == "harsh_brake" ~ "medium",
      TRUE ~ "low"
    )
  )

# -------------------------------
# 4. Time Dimension Attributes
# -------------------------------

processed_data <- processed_data %>%
  mutate(
    hour = hour(timestamp),
    day = day(timestamp),
    month = month(timestamp),
    year = year(timestamp)
  )

# -------------------------------
# 5. Create Dimension Tables
# -------------------------------

# Vehicle Dimension
dim_vehicle <- processed_data %>%
  select(vehicle_id) %>%
  distinct()

# Time Dimension
dim_time <- processed_data %>%
  select(timestamp, hour, day, month, year) %>%
  distinct() %>%
  mutate(time_id = row_number())

# Location Dimension
dim_location <- processed_data %>%
  select(latitude, longitude) %>%
  distinct() %>%
  mutate(location_id = row_number())

# Event Dimension
dim_event <- processed_data %>%
  select(event_type, severity) %>%
  distinct()

# -------------------------------
# 6. Create Fact Table
# -------------------------------

fact_vehicle_events <- processed_data %>%
  left_join(dim_time, by = "timestamp") %>%
  left_join(dim_location, by = c("latitude", "longitude")) %>%
  select(
    vehicle_id,
    time_id,
    location_id,
    acceleration,
    gyroscope,
    speed,
    vibration,
    event_type,
    severity
  )

# -------------------------------
# 7. Save Processed Tables
# -------------------------------

write_csv(dim_vehicle, "data/dim_vehicle.csv")
write_csv(dim_time, "data/dim_time.csv")
write_csv(dim_location, "data/dim_location.csv")
write_csv(dim_event, "data/dim_event.csv")
write_csv(fact_vehicle_events, "data/fact_vehicle_events.csv")

print("ETL Pipeline Completed")