import pandas as pd
from pandera import Column, DataFrameSchema, Check, Float, Int

df = pd.read_csv('crashData.csv', usecols=['Crash ID', 'Record Type', 'Vehicle ID', 'Participant ID', 'Crash Hour', 'Latitude Degrees', 'Longitude Degrees', 'School Zone Indicator', 'Work Zone Indicator', 'Total Vehicle Count', 'Total Count of Persons Involved', 'Total Vehicle Occupant Count', 'Latitude Minutes', 'Latitude Seconds', 'Longitude Minutes', 'Longitude Seconds'])

# Separate csv file into three separate records
crash_df = df[df['Record Type'] == 1]
vehicle_df = df[df['Record Type'] == 2]
person_df = df[df['Record Type'] == 3]

# Assertion 1a
location_schema = DataFrameSchema({
  "lat_col": Column(
    Float, Check(lambda x: x > 0.0, element_wise=True,
      error="Latitude > 0.0")),
  "long_col": Column(
    Float, Check(lambda x: x < 0.0, element_wise=True,
      error="Longitude < 0.0"))
})

location_check_df = pd.DataFrame({
  "lat_col": crash_df['Latitude Degrees'],
  "long_col": crash_df['Longitude Degrees']
})

location_schema(location_check_df)

# Assertion 1b + 2b
time_schema = DataFrameSchema({
  "hour": Column(
    Float, Check(lambda x: 0 <= x <= 23 or x == 99, element_wise=True,
      error="Crash Hour range [0, 23] or 99 if missing"))
})

time_check_df = pd.DataFrame({
  "hour": crash_df['Crash Hour']
})

time_schema(time_check_df)

# Assertion 2a
zone_schema = DataFrameSchema({
  "school_zone": Column(
    Float, Check(lambda x: x == 0 or x == 1, element_wise=True,
      error="Yes or no fields limited to 0 or 1 values")),
  "work_zone": Column(
    Float, Check(lambda x: x == 0 or x == 1, element_wise=True,
      error="Yes or no fields limited to 0 or 1 values"))
})

zone_check_df = pd.DataFrame({
  "school_zone": crash_df['School Zone Indicator'],
  "work_zone": crash_df['Work Zone Indicator']
})

zone_schema(zone_check_df)

# Assertion 3a

for crash_id in vehicle_df.drop_duplicates(subset=['Vehicle ID'])['Crash ID'].values:
  if not (crash_df['Crash ID'] == crash_id).any():
    print('crash ' + str(crash_id) + ' not found for all vehicles')

for crash_id in person_df.drop_duplicates(subset=['Participant ID'])['Crash ID'].values:
  if not (crash_df['Crash ID'] == crash_id).any():
    print('crash ' + str(crash_id) + ' not found for some participant')
'''
for vehicle_id in person_df.drop_duplicates(subset=['Participant ID'])['Vehicle ID'].values:
  if not (vehicle_df['Vehicle ID'] == vehicle_id).any():
    print('vehicle ' + str(vehicle_id) + ' not found for some participant')
'''
# Assertion 3b

all_count_df = pd.DataFrame({
  "Crash ID": df['Crash ID'],
  "Vehicle ID": df['Vehicle ID'],
  "Participant ID": df['Participant ID'],
  "v_count": df['Total Vehicle Count'],
  "p_count": df['Total Count of Persons Involved'],
  "occ_count": df['Total Vehicle Occupant Count']
})

print('vehicles: ' + str(all_count_df['v_count'].sum()))
print('unique vehicle IDs: ' + str(len(df.drop_duplicates(subset=['Vehicle ID']))))
print('vehicle occupants: ' + str(all_count_df['occ_count'].sum()))
print('total persons: ' + str(all_count_df['p_count'].sum() - 9*(all_count_df['p_count'] == 9).sum()))
print('unique participant IDs: ' + str(len(df.drop_duplicates(subset=['Participant ID']))))
print('total participant IDs: ' + str(len(df['Participant ID'])))

# Assertion 4a

if len(crash_df) == len(crash_df.drop_duplicates(subset=['Crash ID'])):
  print('All crash_ids are unique, not duplicates found')

# Assertion 4b

print(len(df))
loc_df = pd.DataFrame({
  "Min": crash_df['Latitude Minutes'],
  "Sec": crash_df['Latitude Seconds']
})
same_loc_df = loc_df[loc_df.groupby('Min')['Sec'].transform('nunique').gt(1)]
print(same_loc_df)
