# Retail Rocket data attribution

Dataset: Retailrocket recommender system dataset.

Creator: Retail Rocket. Source: https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset

Dataset license reported by Kaggle metadata on 10 October 2026: Creative Commons Attribution NonCommercial ShareAlike 4.0 International.

License text: https://creativecommons.org/licenses/by-nc-sa/4.0/

`events_sample.csv` is the earliest 5,000 records selected from the original `events.csv`, sorted by timestamp. Identifiers and event values are unchanged. It contains 4,879 views, 98 add-to-cart events and 23 transactions. Selection and sorting are the only changes. This sample and derived data retain the dataset license. Use for this educational project is noncommercial. The code license does not relicense the dataset.

`scripts/live_demo.py` and acceptance tests generate clearly labeled mock events. Those events are not presented as observations from Retail Rocket.

The downloadable submission ZIP also includes the unchanged original `retailrocket_events.csv.zip` archive under this same dataset license. Git intentionally excludes that binary to avoid a large repository; Git users can obtain it using `scripts/download_data.py`. No Kaggle credentials are stored in the project.

The bundle's full `events.csv` contains all 2,756,101 original events after validation and stable sorting by timestamp. Original order breaks timestamp ties. No event fields are changed. This sorted derivative retains the same Creative Commons Attribution NonCommercial ShareAlike 4.0 license and is also excluded from Git.
