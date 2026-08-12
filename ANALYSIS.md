## 📋 Job Descriptions & Technical Logic

The analytics pipeline executes 8 comprehensive business intelligence queries in parallel across the cluster:

* **Job 1: Total Bookings per Event**
  * *Logic:* Joins `events` and `seats`, groups by `event_id`, and counts total valid seat IDs. Includes events with 0 bookings using a `left` join and `na.fill()`.
* **Job 2: Seat Occupancy Percentage per Event**
  * *Logic:* Calculates `(Booked Seats / Total Venue Capacity) * 100` for each event, handling zero-occupancy events gracefully.
* **Job 3: Total Revenue per Event**
  * *Logic:* Groups `seats` by event and calculates the sum of the `price` column, rounded to 2 decimal places.
* **Job 4: Number of Available Seats per Event**
  * *Logic:* Computes remaining capacity based on standard venue limits minus total bookings per screening.
* **Job 5: Top 5 Most-Booked Events**
  * *Logic:* Reuses the DataFrame from Job 1, applies an `orderBy` descending sort on `total_bookings`, and applies a `.limit(5)` transformation.
* **Job 6: Booking Statistics by Event Category**
  * *Logic:* Joins `events` and `seats`, grouping by the event genre/category to aggregate total bookings and revenue by genre.
* **Job 7: Booking Statistics by Date**
  * *Logic:* Parses the `screen_time` timestamp into a standard Date type. Groups total ticket counts and revenue sums by this distinct daily date.
* **Job 8: Top 5 Users by Number of Bookings**
  * *Logic:* Joins `users` and `seats`, groups by `user_id`, and ranks the top 5 most active customers by ticket volume.
