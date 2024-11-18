import datetime

# Convert microseconds to seconds
timestamp_in_seconds = 1731935868769 / 1_000

# Convert to a datetime object
human_readable_time = datetime.datetime.fromtimestamp(timestamp_in_seconds)

print(human_readable_time)