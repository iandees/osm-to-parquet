[out:json][timeout:25];
(
  way["highway"]({{bbox}});
  - way["highway"="service"]({{bbox}});
);
out ids;
