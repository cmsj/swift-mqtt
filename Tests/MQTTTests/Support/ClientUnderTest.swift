import Foundation
@testable import MQTT

/// What a behaviour test needs from an MQTT client, independent of the client's API shape.
///
/// The behaviour tests talk only to this protocol. When the client implementation
/// changes, only `makeClient(version:endpoint:)` and its adapter change; the tests stay put.
protocol ClientUnderTest: Sendable {
    var version: Version { get }
    var events: ClientEvents { get }
    var status: Status { get async }

    func open(_ options: OpenOptions) async throws -> OpenResult
    /// Returns the reason code of the final acknowledgement, or `nil` for QoS 0.
    func publish(_ topic: String, _ payload: String, qos: MQTTQoS, retain: Bool) async throws -> UInt8?
    func subscribe(_ topics: [String], qos: MQTTQoS) async throws -> [UInt8]
    func unsubscribe(_ topics: [String]) async throws -> [UInt8]
    func close() async throws
    /// Stops reconnecting and releases the connection, ignoring errors. Used for test cleanup.
    func shutdown() async
}

extension ClientUnderTest {
    func publish(_ topic: String, _ payload: String, qos: MQTTQoS = .atLeastOnce) async throws -> UInt8? {
        try await publish(topic, payload, qos: qos, retain: false)
    }
}

struct OpenOptions: Sendable {
    struct Retry: Sendable {
        var interval: TimeInterval = 0.1
        var limit: UInt = 5
    }

    var clientId = "test-client"
    var username: String?
    var password: String?
    var cleanStart = true
    var keepAlive: UInt16 = 60
    /// Off by default so keep-alive pings don't interleave with the packets a test expects.
    var pingEnabled = false
    var pingTimeout: TimeInterval = 5
    var publishTimeout: TimeInterval = 5
    var retry: Retry?
    /// v5 enhanced authentication method, sent in CONNECT.
    var authMethod: String?
    var authflow: (@Sendable (Auth) async throws -> Auth)?
}

struct OpenResult: Sendable, Equatable {
    var sessionPresent: Bool
    var reasonCode: UInt8
}

struct ReceivedMessage: Sendable, Equatable {
    var topic: String
    var payload: String
    var qos: MQTTQoS
    var retain: Bool
}

/// Records status changes and inbound messages in the order the client reports them.
final class ClientEvents: Sendable {
    private let statuses = Mailbox<Status>("status change")
    private let messages = Mailbox<ReceivedMessage>("message")

    func record(_ status: Status) { statuses.push(status) }
    func record(_ message: ReceivedMessage) { messages.push(message) }

    func nextStatus(within timeout: Duration = .prompt) async throws -> Status {
        try await statuses.next(within: timeout)
    }

    func nextMessage(within timeout: Duration = .prompt) async throws -> ReceivedMessage {
        try await messages.next(within: timeout)
    }

    /// Skips status changes until one matches, returning it.
    func waitForStatus(within timeout: Duration = .prompt, where matches: (Status) -> Bool) async throws -> Status {
        let deadline = ContinuousClock.now + timeout
        while true {
            let status = try await nextStatus(within: deadline - .now)
            if matches(status) { return status }
        }
    }
}

extension Status {
    var isClosed: Bool {
        if case .closed = self { return true }
        return false
    }

    var closeReason: CloseReason? {
        if case .closed(let reason) = self { return reason }
        return nil
    }
}

// MARK: Harness

/// The one place that decides which client implementation the tests exercise.
func makeClient(version: Version, endpoint: Endpoint) -> any ClientUnderTest {
    LegacyClient(version: version, endpoint: endpoint)
}

struct Harness: Sendable {
    let broker: FakeBroker
    let client: any ClientUnderTest

    var version: Version { client.version }
    var protocolLevel: UInt8 { version.uint8 }

    /// Opens the client and completes the handshake, returning the broker's side of the connection.
    func connect(
        _ options: OpenOptions = .init(),
        sessionPresent: Bool = false,
        properties: [[UInt8]] = []
    ) async throws -> BrokerConnection {
        async let opened = client.open(options)
        let connection = try await broker.nextConnection()
        try await connection.acceptConnect(sessionPresent: sessionPresent, properties: properties)
        _ = try await opened
        return connection
    }
}

/// Runs `body` against a fresh fake broker and client, cleaning both up afterwards.
func withHarness(_ version: Version, _ body: (Harness) async throws -> Void) async throws {
    let broker = try await FakeBroker.start()
    let harness = Harness(broker: broker, client: makeClient(version: version, endpoint: .tcp(host: "127.0.0.1", port: broker.port)))
    do {
        try await body(harness)
    } catch {
        await harness.client.shutdown()
        broker.stop()
        throw error
    }
    await harness.client.shutdown()
    await broker.stop()
}
