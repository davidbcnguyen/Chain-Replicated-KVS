# Chain-Replicated Key-Value Store
Key-value store that is failure resistant and eventually consistent

## Build
`go build`

`make all`

## Running
Make sure that the addressses in the config folder are all correct.

First run the coordinator node that will coordinate all the workers

`./bin/coord`

Then run the server nodes that will store the data.

`./bin/server`

Now, the KVS is ready to accept requests from clients. You can run the clients with

`./bin/client`