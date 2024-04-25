[![progress-banner](https://backend.codecrafters.io/progress/redis/b6ade902-e7be-41a0-bb10-4f78638058db)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a starting point for Go solutions to the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

In this challenge, you'll build a toy Redis clone that's capable of handling
basic commands like `PING`, `SET` and `GET`. Along the way we'll learn about
event loops, the Redis protocol and more.

## Currently implemented
These are the current redis commands implemented

```sh

$ redis-cli ping
PONG

$ redis-cli echo test
test

$ redis-cli set test pog
OK

$ redis-cli get test
"pog"

```
---
## How to run

Requires

* Go version go1.22.2
* redis-cli 6.0.16
---
Just run `./spawn_redis_server.sh` and you are in!

![alt wicked](https://cdn.betterttv.net/emote/5f9487731b017902db14d05e/3x.webp)
```sh
$ ./spawn_redis_server.sh

2024/04/25 15:48:09 Accepting connections

```
---
## Stages done

| Task                         | Status   |
|------------------------------|----------|
| Bind to a port               | ✅        |
| Respond to PING              | ✅        |
| Respond to multiple PINGs    | ✅        |
| Handle concurrent clients    | ✅        |
| Implement the ECHO command   | ✅        |
| Implement the SET & GET commands | ✅    |
| Expiry                       | ✅        |
