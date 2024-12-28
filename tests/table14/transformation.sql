select "LocationID",upper("Borough") as Borough,"Zone","service_zone" from NY_YELLO_TAXI.qa.trip_lookup_raw
 where "LocationID" <=100