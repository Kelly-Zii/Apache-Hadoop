# IT5100B 2023 - Project 2


GenerateVehicleLocation:A Kafka Producer that simulates vehicle location updates and publishes them to a Kafka topic
"events."

CountVehicleStops/LongDistanceTravellers: Kafka Streams applications that processes the location updates from the "events" topics, groups them by unique stops, counts the stops and identifies long-distance travellers based on the number of unique stops visited in a given time window.

set W*H in GenerateVehicleLocation.scala
set T in CountVehicleStops.scala and LongDistanceTravellerss.scala

1.Preview of generator

<img width="828" alt="image" src="https://user-images.githubusercontent.com/122529996/233833206-25ee61e1-205d-467e-b5f9-9bbde6cab01e.png">

2. Preview of LongDistanceTravellers
<img width="1195" alt="image" src="https://github.com/Kelly-Zii/Apache-Hadoop/assets/122529996/f62ff907-1e00-40b0-a02f-b10b893f31af">

above image shows that:
Traveller [TravllerID] is a longdistancetraveller : value(place it travelled)
