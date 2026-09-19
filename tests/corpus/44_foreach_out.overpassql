[out:json][timeout:25];
node[amenity=cafe]({{bbox}})->.a;
foreach.a->.b
{
  .b out;
}
