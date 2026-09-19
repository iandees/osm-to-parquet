[out:json][timeout:25];
(
  node["leisure"="park"]({{bbox}});
  way["leisure"="park"]({{bbox}});
  relation["leisure"="park"]({{bbox}});
);
out body;
>;
out skel qt;
