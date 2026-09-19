[out:json][timeout:25];
way["highway"]({{bbox}})->.a;
way["name"]({{bbox}})->.b;
way.a.b;
out ids;
