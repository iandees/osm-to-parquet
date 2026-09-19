[out:json][timeout:25];
(
  node["shop"]({{bbox}});
  way["shop"]({{bbox}});
  relation["shop"]({{bbox}});
);
out body;
>;
out skel qt;
