import Foundation
@testable import MQTT

/// Adapts the `MQTTClient` actor to `ClientUnderTest`.
struct ActorClient: ClientUnderTest {
    let version: Version
    let events = ClientEvents()
    private let client: MQTTClient
    private let forwarding: Task<Void, Never>

    init(version: Version, endpoint: Endpoint) {
        self.version = version
        client = MQTTClient(endpoint, version: version)
        let stream = client.events()
        forwarding = Task { [events] in
            for await event in stream {
                switch event {
                case .status(let status, _):
                    events.record(status)
                case .message(let message):
                    events.record(ReceivedMessage(
                        topic: message.topic,
                        payload: String(decoding: message.payload, as: UTF8.self),
                        qos: message.qos,
                        retain: message.retain
                    ))
                case .error:
                    break
                }
            }
        }
    }

    var status: Status {
        get async { await client.status }
    }

    func open(_ options: OpenOptions) async throws -> OpenResult {
        var config = Config()
        config.keepAlive = options.keepAlive
        config.pingEnabled = options.pingEnabled
        config.pingTimeout = options.pingTimeout
        config.requestTimeout = options.requestTimeout
        config.retrier = options.retry.map { Retrier(.equals(interval: $0.interval), limits: $0.limit) }
        await client.configure(config)
        let connack = try await client.open(
            Identity(options.clientId, username: options.username, password: options.password),
            cleanStart: options.cleanStart,
            properties: options.authMethod.map { [.authenticationMethod($0)] } ?? [],
            authflow: options.authflow
        )
        return OpenResult(sessionPresent: connack.sessionPresent, reasonCode: connack.code.rawValue)
    }

    func publish(_ topic: String, _ payload: String, qos: MQTTQoS, retain: Bool) async throws -> UInt8? {
        try await client.publish(to: topic, payload: payload, qos: qos, retain: retain)?.code.rawValue
    }

    func subscribe(_ topics: [String], qos: MQTTQoS) async throws -> [UInt8] {
        try await client.subscribe(to: topics.map { Subscribe($0, qos: qos) }).codes.map(\.rawValue)
    }

    func unsubscribe(_ topics: [String]) async throws -> [UInt8] {
        try await client.unsubscribe(from: topics).codes.map(\.rawValue)
    }

    func close() async throws {
        try await client.close()
    }

    func shutdown() async {
        try? await client.close()
        forwarding.cancel()
    }
}
