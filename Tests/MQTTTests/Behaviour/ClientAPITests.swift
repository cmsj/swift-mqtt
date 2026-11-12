import Foundation
import Testing
@testable import MQTT

/// Behaviour specific to the `MQTTClient` actor API, beyond what `ClientUnderTest` covers.
@Suite(.timeLimit(.minutes(1)))
struct ClientAPITests {
    /// Runs `body` with a fake broker and a client pointed at it, closing both afterwards.
    private func withClient(
        _ version: Version = .v5_0,
        _ body: (FakeBroker, MQTTClient) async throws -> Void
    ) async throws {
        let broker = try await FakeBroker.start()
        defer { broker.stop() }
        let client = MQTTClient(.tcp(host: "127.0.0.1", port: broker.port), version: version)
        do {
            try await body(broker, client)
        } catch {
            try? await client.close()
            throw error
        }
        try? await client.close()
    }

    private func open(_ client: MQTTClient, on broker: FakeBroker) async throws -> BrokerConnection {
        async let opened = client.open(Identity("api-test"))
        let connection = try await broker.nextConnection()
        try await connection.acceptConnect()
        _ = try await opened
        return connection
    }

    @Test
    func cancellingPublishThrowsCancellationAndKeepsConnection() async throws {
        try await withClient { broker, client in
            let connection = try await open(client, on: broker)
            let publishing = Task { try await client.publish(to: "a/b", payload: "x", qos: .atLeastOnce) }
            _ = try await connection.expectPublish()
            publishing.cancel()
            await #expect(throws: CancellationError.self) {
                try await publishing.value
            }
            #expect(await client.status == .opened)
        }
    }

    @Test
    func closingWhileOpeningFailsOpen() async throws {
        try await withClient { broker, client in
            let opening = Task { try await client.open(Identity("api-test")) }
            let connection = try await broker.nextConnection()
            _ = try await connection.expectConnect()
            try await client.close()
            await #expect(throws: MQTTError.clientClose(.normal)) {
                try await opening.value
            }
            #expect(await client.status == .closed(.clientClose(.normal)))
        }
    }

    @Test
    func v3RejectsV5OnlyArguments() async throws {
        try await withClient(.v3_1_1) { broker, client in
            await #expect(throws: MQTTError.packetError(.badParameter)) {
                try await client.open(Identity("api-test"), properties: [.sessionExpiryInterval(60)])
            }
            _ = try await open(client, on: broker)
            await #expect(throws: MQTTError.packetError(.badParameter)) {
                try await client.publish(to: "a/b", payload: "x", properties: [.contentType("text/plain")])
            }
            await #expect(throws: MQTTError.packetError(.badParameter)) {
                try await client.auth([.authenticationMethod("m")])
            }
        }
    }

    @Test
    func everyEventStreamSeesEveryEvent() async throws {
        try await withClient { broker, client in
            let first = client.events()
            let second = client.events()
            let connection = try await open(client, on: broker)
            connection.send(BrokerEncoder.publish(topic: "a/b", payload: "hi", protocolLevel: 5))

            for stream in [first, second] {
                var kinds: [String] = []
                for await event in stream {
                    switch event {
                    case .status(let status, _): kinds.append("\(status)")
                    case .message(let message): kinds.append(message.topic)
                    case .error: kinds.append("error")
                    }
                    if kinds.count == 3 { break }
                }
                #expect(kinds == ["opening", "opened", "a/b"])
            }
        }
    }

    @Test
    func willIsSentInConnect() async throws {
        try await withClient { broker, client in
            let will = Will(topic: "status/api-test", payload: Data("offline".utf8), retain: true)
            async let opened = client.open(Identity("api-test"), will: will)
            let connection = try await broker.nextConnection()
            let connect = try await connection.acceptConnect()
            _ = try await opened
            #expect(connect.willTopic == "status/api-test")
        }
    }
}
