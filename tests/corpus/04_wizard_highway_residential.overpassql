[out:json][timeout:25];
(
  node["highway"="residential"]({{bbox}});
  way["highway"="residential"]({{bbox}});
  relation["highway"="residential"]({{bbox}});
);
out body;
>;
out skel qt;
