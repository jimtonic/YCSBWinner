CREATE TABLE samples (
  metric        text,
  date          text,
  event_time  timestamp,
  value         blob,
  attributes    map<text, text>,
  PRIMARY KEY((metric, date), event_time)
);