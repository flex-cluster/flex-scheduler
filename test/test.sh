curl -X POST http://localhost:8066/schedule \
     -H "Content-Type: application/json" \
     -d '{
           "ComRequired": {
             "cpu": 500,
             "memory": 128
           },
           "DeviceRequired": {
             "device": 1
           }
         }'

