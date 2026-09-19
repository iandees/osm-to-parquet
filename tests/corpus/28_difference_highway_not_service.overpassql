[out:json][timeout:180];
(
  way["highway"]({{bbox}});
  - way["highway"="service"]({{bbox}});
);
out ids;
