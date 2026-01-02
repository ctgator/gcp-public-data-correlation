CREATE TABLE IF NOT EXISTS `pub-data-correlation-dev.raw.raw_pageviews`
(
  project STRING,
  article STRING,
  views INT64,
  date DATE,
  hour INT64,
  ingested_at TIMESTAMP
)
PARTITION BY date
CLUSTER BY project, article;

