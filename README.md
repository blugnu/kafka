<div align="center" style="margin-bottom:20px">
  <img src=".assets/banner.png" alt="ulog" />
  <h1><i>making Kafka less Kafka-esque</i></h1>
  <div align="center">
    <a href="https://github.com/blugnu/kafka/actions/workflows/release.yml">
        <img alt="build-status" src="https://github.com/blugnu/kafka/actions/workflows/pipeline.yml/badge.svg?branch=master&style=flat-square"/>
    </a>
    <a href="https://goreportcard.com/report/github.com/blugnu/kafka" >
        <img alt="go report" src="https://goreportcard.com/badge/github.com/blugnu/kafka"/>
    </a>
    <a>
        <img alt="go version >= 1.14" src="https://img.shields.io/github/go-mod/go-version/blugnu/kafka?style=flat-square"/>
    </a>
    <a href="https://github.com/blugnu/kafka/blob/master/LICENSE">
        <img alt="MIT License" src="https://img.shields.io/github/license/blugnu/kafka?color=%234275f5&style=flat-square"/>
    </a>
    <a href="https://coveralls.io/github/blugnu/kafka?branch=master">
        <img alt="coverage" src="https://img.shields.io/coveralls/github/blugnu/kafka?style=flat-square"/>
    </a>
    <a href="https://pkg.go.dev/github.com/blugnu/kafka">
        <img alt="docs" src="https://pkg.go.dev/badge/github.com/blugnu/kafka"/>
    </a>
  </div>
</div>

<br>

# blugnu/kafka

## Features

- [x] **Discoverable Configuration**: Provides option functions for configuring Kafka clients,
  with separation of general, consumer and producer-specific configuration

- [x] **Reduced Boilerplate**: Provides a complete implementation of a Kafka consumer with
  a single function call, handling all the boilerplate code for you including offset
  commits, signal handling and graceful shutdown

- [x] **Producer Retries**: Provides a producer implementation that will retry sending
  messages to Kafka in the event of a timeout (configurable timeout and `MaxRetries`)

- [x] **Mock Producer**: Provides a mock producer implementation that can be used
  for testing that applications produce expected messages

## Installation

```bash
go get github.com/blugnu/kafka
```

## Usage

- Establish a base configuration (e.g. bootstrap servers)
- Configure a consumer and/or producer
- Start the consumer

## Example

```go
package main

import (
  "context"
  "fmt"
  "os"
  "os/signal"
  "syscall"

  "github.com/blugnu/kafka"
)

func HandleEvent(ctx context.Context, msg *kafka.Message) error {
  fmt.Printf("received message: %s\n", string(msg.Value))
  return nil
}

func main() {
  // initialise a base configuration
  cfg := kafka.NewConfig(
    kafka.BootstrapServers("localhost:9092"),
  )

  // configure a consumer
  consumer, err := kafka.NewConsumer(cfg,
    kafka.ConsumerGroupID("my-group"),
    kafka.TopicHandler("event", kafka.HandlerFunc(HandleEvent)),
  )
  if err != nil {
    log.Fatal("error creating consumer:", err)
  }

  // start the consumer
  if err := consumer.Start(ctx); err != nil {
    log.Fatal(err)
  }

  if err := consumer.Wait(); err != nil {
    log.Fatal(err)
  }
}
```

# Logging

To avoid importing a specific logging library or imposing a log format on applications,
logs are written using internal log hooks.  These are set to no-op by default.

To enable logging you must call the `kafka.EnableLogs` function, providing functions
to log entries at different levels as required.

For example, the following might be used to initialise a `blugnu/ulog` context logger
and enable logging of `INFO` level Kafka logs to that logger; logs at all other levels
are left as no-ops:

```go
func logger(ctx context.Context) (context.Context, ulog.Logger, func()) {
    log, cfn, err := ulog.NewLogger(
        ulog.WithLevel(ulog.DebugLevel),
    )
    if err != {
        log.Fatal(fmt.Errorf("error initialising logger: %v", err))
    }

    kafka.EnableLogs(&kafka.Loggers{
        Info: func(ctx context.Context, msg string, fields map[string]interface{}) {
            log := ulog.FromContext(ctx)
            log.Info(msg, ulog.Fields(fields))
        },
    })

    return ulog.ContextWithLogger(ctx, log), log, cfn
}
```

Logging functions are provided for each of the following levels:

- `Debug`
- `Info`
- `Error`

## Default Logging

A `nil` argument may be passed to `EnableLogs` to enable default logging, which will
write logs using the standard `log` package.

Default logging is also emitted if a zero-value `&Loggers{}` is passed to `EnableLogs`, i.e.
all functions set `nil`.

> **Note**: Default logging is not recommended for production use.
