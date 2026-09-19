[out:json][timeout:25];
(
  node["building"]({{bbox}});
  way["building"]({{bbox}});
  relation["building"]({{bbox}});
);
out body;
>;
out skel qt;
