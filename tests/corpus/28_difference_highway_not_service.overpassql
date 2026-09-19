[out:json][timeout:180][maxsize:1073741824];
(
  way["highway"]({{bbox}});
  - way["highway"="service"]({{bbox}});
);
out ids;
