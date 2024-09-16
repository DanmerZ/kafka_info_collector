Programming task: Spring Boot Kafka Information Collector
Max time for solving: 60min
Goal: Consume a Kafka stream of events and collect certain information in
memory to be able to deliver it to a caller via RESTful API.
The event should contain at least one field containing a mail address. The
application should consume the event stream and count the unique number of
overall mail addresses and domains which occurred within the event stream.
Bring in own ideas when something seems not be fully specified.

### Run

```
docker compose up -d
```

Check the emails and domains count
```
curl localhost:8081
``` 
