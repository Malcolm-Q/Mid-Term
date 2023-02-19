# Queries and Thoughts
We will create two CSV's. One containing flights that were on time or early and one containing flights that are late.
```SQL
SELECT * FROM 
(   
    SELECT
    fl_date,
    mkt_unique_carrier,
    mkt_carrier_fl_num,
    op_unique_carrier,
    tail_num,
    op_carrier_fl_num,
    origin_city_name,
    dest_city_name,
    crs_dep_time,
    crs_arr_time,
    crs_elapsed_time,
    distance,
    (arr_delay + dep_delay) 
    AS total_delay
    FROM flights
    WHERE arr_delay + dep_delay > 0 AND arr_delay + dep_delay < 301
)
AS sub_query
WHERE fl_date LIKE '2019-01%'
ORDER BY fl_date ASC
LIMIT 100000
```
This is the main query. To get flights on time we can change the first where statement to be:
```SQL
WHERE arr_delay + dep_delay < 1 AND arr_delay + dep_delay IS NOT NULL
```
## Notes and Observations
There are some rows where arr_delay + dep_delay are null. These are cancelled or diverted flights. We will exclude these for now as they're outliers.

Each month has a ton of data. **Taking 100,000 rows gives us just over one week of data** (for January at least). We're going to expirement with this approach first. 100,000 rows for a given month with information on each day of the week will return stronger patterns than 100,000 rows with ~8,333 for each month will. It will also run faster than a 1.2m row database with 100k rows will. Once we have a process for building a successful model it will be fast and easy to set up 12 models for each month if needed.

In the query to grab delayed flights we cap the included delay time at 301. The majority fall within this range (most being under 100 min) but there are some very high occurences (1400+ mins/24+ hours). Unlike in most other countries it seems like North American airlines have [no legal obligation regarding compensation for delayed flights](https://www.transportation.gov/individuals/aviation-consumer-protection/flight-delays-cancellations) other than regular updates. Each airline chooses their own policies, case by case, or even customer by customer. Of course in the age of customer satisfaction many are very generous. However there is a legal obligation to compensate for cancelled flights unless they prove the circumstances are 'extraordinary'. This results in airlines choosing to delay flights by absurd amounts instead of cancelling them. The standard in most countries seems to be three hours for domestic flights and six hours for international. All flights in the data are domestic and [the DOT now enforces minor compensation](https://www.thrillist.com/news/nation/flight-delay-refunds-department-of-transportation-rules) for delays over 3 hours. **We assume any delay over 300 minutes is essentially a cancel.**

