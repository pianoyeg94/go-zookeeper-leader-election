# go-zookeeper-leader-election

This package is intended to be used with serveral instances, so that it can provide fault tolerance in mission critical scenarios. Try launching 3 instances of the bellow main.go, and you will see, that only one instance will become leader.

You can experiment by taking some of the main.go instances down and observing that the leadership transfers correctly from one instance to another. You can also shut down 2 of the zookeeper instances, to see that the main.go services will keep trying to reconnect. As soon as you bring at least another zookeeper instance up to form a quorum, the main.go services will restore their session and proceed with their leader and follower activities.

#### Battle tested in production environments, BUT still LACKING:
- Excessive unit and integration test coverage, especially for corner cases;
- Comments and documentation (though the example of usage bellow is in itself a self-contained API documentation);
- Monitoring and logging hooks (callbacks).

#### docker-compose.yaml
```yaml
version: '3.9'

services:
  zookeeper-1:
    image: confluentinc/cp-zookeeper:7.0.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SERVER_ID: 1
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888;zookeeper-2:2888:3888;zookeeper-3:2888:3888
      ZOOKEEPER_TICK_TIME: 2000
      ZOOKEEPER_INIT_LIMIT: 10
      ZOOKEEPER_SYNC_LIMIT: 5
    volumes:
      - zookeeper-1-data:/var/lib/zookeeper/data
    ports:
      - 22181:2181 

  zookeeper-2:
    image: confluentinc/cp-zookeeper:7.0.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SERVER_ID: 2
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888;zookeeper-2:2888:3888;zookeeper-3:2888:3888
      ZOOKEEPER_TICK_TIME: 2000
      ZOOKEEPER_INIT_LIMIT: 10
      ZOOKEEPER_SYNC_LIMIT: 5
    volumes:
      - zookeeper-2-data:/var/lib/zookeeper/data
    ports:
      - 22182:2181 

  zookeeper-3:
    image: confluentinc/cp-zookeeper:7.0.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SERVER_ID: 3
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888;zookeeper-2:2888:3888;zookeeper-3:2888:3888
      ZOOKEEPER_TICK_TIME: 2000
      ZOOKEEPER_INIT_LIMIT: 10
      ZOOKEEPER_SYNC_LIMIT: 5
    volumes:
      - zookeeper-3-data:/var/lib/zookeeper/data
    ports:
      - 22183:2181 

volumes:
  zookeeper-1-data: 
  zookeeper-2-data:
  zookeeper-3-data:
```

#### main.go
```go
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-zookeeper/zk"
	leaderelection "github.com/pianoyeg94/go-zookeeper-leader-election"
)

const (
	namespace = "leaderelection"

	sessionTimeout = 10 * time.Second
)

var (
	id      int64
	servers = [...]string{"localhost:22181", "localhost:22182", "localhost:22183"}
)

func init() {
	flag.Int64Var(&id, "id", 1, "zookeeper client id")
	flag.Parse()
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	logger := log.New(os.Stderr, "leaderelection: ", log.LstdFlags|log.Lmsgprefix)
	if err := run(ctx); err != nil {
		logger.Fatalln(err)
	}
}

func run(ctx context.Context) error {
	errs := make(chan error, 1)
	election := leaderelection.NewLeaderElection(id, namespace, servers[:], sessionTimeout)
	defer func() {
		election.Resign()
		for range errs {
		}
	}()

	go func() {
		defer close(errs)
		errs <- election.Join(NewLeader("I'm leading"), leaderelection.FollowerRoutine(followerRoutine))
	}()

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return nil
	}
}

func NewLeader(msg string) *Leader {
	return &Leader{msg}
}

type Leader struct {
	msg string
}

func (l *Leader) Lead(ctx context.Context, _ *zk.Conn) error {
	for {
		select {
		case <-time.After(1 * time.Second):
			log.Println(l.msg)
		case <-ctx.Done():
			return nil
		}
	}
}

func followerRoutine(ctx context.Context, _ *zk.Conn) error {
	for {
		select {
		case <-time.After(1 * time.Second):
			log.Println("I'm following")
		case <-ctx.Done():
			return nil
		}
	}
}
```

