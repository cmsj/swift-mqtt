# swift-mqtt
![Platform](https://img.shields.io/badge/platforms-macOS%2027-F28D00.svg)

An MQTT 3.1.1 and 5.0 client for macOS over TCP, TLS, WebSocket and QUIC, built on
Network.framework and Swift concurrency.

This is a fork of [emqx/swift-mqtt](https://github.com/emqx/swift-mqtt) for Hammerspoon 2. The client is an `actor` with an
`async` API, has no dependencies, and is checked under Swift 6 strict concurrency
with no `@unchecked Sendable`.

## Requirements

- macOS 27+
- Swift 6.2+

## Integration

```swift
// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "YOUR_PROJECT_NAME",
    dependencies: [
        .package(url: "https://github.com/cmsj/swift-mqtt.git", branch: "main"),
    ]
)
```

## Usage

```swift
import MQTT

var config = Config()
config.keepAlive = 60
// Reconnect after unexpected disconnects, except when the broker is busy.
config.retrier = Retrier(.exponential(), limits: 10) { reason in
    if case .serverClose(.serverBusy) = reason { return true }
    return false
}
config.monitorsNetwork = true

let client = MQTTClient(.tls(host: "broker.emqx.io", tls: .trustAll()), version: .v5_0, config: config)

// Status changes, messages and errors, in order. Every call to events() gets its own stream.
Task {
    for await event in client.events() {
        switch event {
        case .status(let status, let previous):
            print("status: \(previous) -> \(status)")
        case .message(let message):
            print("\(message.topic): \(String(decoding: message.payload, as: UTF8.self))")
        case .error(let error):
            print("error: \(error)")
        }
    }
}

try await client.open(Identity("my-client", username: "user", password: "secret"))
try await client.subscribe(to: "sensors/#", qos: .atLeastOnce)
let ack = try await client.publish(to: "sensors/kitchen", payload: "21.5", qos: .exactlyOnce)
try await client.unsubscribe(from: "sensors/#")
try await client.close()
```

Every operation throws `MQTTError` on failure. Acknowledged requests time out after
`Config.requestTimeout`; cancelling the calling task cancels the wait.

### Endpoints

```swift
.tcp(host: "example.com")                        // port 1883
.tls(host: "example.com", tls: TLSOptions())     // port 8883
.ws(host: "example.com")                         // ws://example.com:8083/mqtt
.wss(host: "example.com", tls: TLSOptions())     // port 8084
.quic(host: "example.com", tls: TLSOptions())    // port 14567
```

`TLSOptions` sets server trust (system, custom roots, or a validation closure), a client
certificate from a PKCS#12 archive, the server name and the TLS version range.

## Testing

```sh
swift test
```

The suite runs against an in-process fake broker and needs no network. To also run the
end-to-end tests over every transport against a real broker:

```sh
MQTT_TEST_HOST=broker.emqx.io swift test --filter BrokerIntegrationTests
```

## License

`swift-mqtt` is available under the Apache 2.0 license. See the [LICENSE](LICENSE) file for more info.
