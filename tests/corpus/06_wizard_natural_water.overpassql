[out:json][timeout:25];
(
  node["natural"="water"]({{bbox}});
  way["natural"="water"]({{bbox}});
  relation["natural"="water"]({{bbox}});
);
out body;
>;
out skel qt;
