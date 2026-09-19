[out:json][timeout:180][maxsize:1073741824];
(
  node["highway"="residential"]({{bbox}});
  way["highway"="residential"]({{bbox}});
  relation["highway"="residential"]({{bbox}});
);
out body;
>;
out skel qt;
