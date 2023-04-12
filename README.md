# store_monitor

## Compute_uptime_downtime logic

This function computes the uptime and downtime for a each store based on its available timestamp data(timstamps that fall within the business hours). The function iterates through the timestamps for each store and calculates the uptime and downtime in the last hour, day, and week.

- It first iterate through the timestamps and extracts the timestamp time and day.

- It checks if the store was active during the business hours for that day. If so, it checks if the store was active or inactive during the current hour.

- If the store was active, it updates the uptime and downtime for the current hour, day, and week based on the timestamp data.

- If the store was inactive during the current hour, it updates the downtime for the current hour and day.

- If the timestamp is older than the current hour, it updates the uptime and downtime for the last hour, and updates the downtime for the day and week.

If the store was inactive during the business hours, it also updates the downtime for the current hour, day, and week based on the available timestamps.

- If the timestamp is from the current day, it updates the downtime and uptime for the day.

- If the timestamp is from the last week, it updates the downtime and uptime for the week.
