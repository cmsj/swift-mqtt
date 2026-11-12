import Foundation
@testable import MQTT

/// Adapts the Promise-based `MQTTClient.V3` / `MQTTClient.V5` classes to `ClientUnderTest`.
/// Deleted together with the legacy client.
struct LegacyClient: ClientUnderTest {
    let version: Version
    let events = ClientEvents()
    /// Serial, so delegate callbacks reach `events` in the order the client sends them.
    private let delegateQueue = DispatchQueue(label: "legacy-client-delegate")
    private let client: MQTTClient
    private let delegate: LegacyDelegate

    init(version: Version, endpoint: Endpoint) {
        self.version = version
        client = switch version {
        case .v3_1_1: MQTTClient.V3(endpoint)
        case .v5_0: MQTTClient.V5(endpoint)
        }
        delegate = LegacyDelegate(events: events)
        client.delegateQueue = delegateQueue
        client.delegate = delegate
    }

    private var v3: MQTTClient.V3 { client as! MQTTClient.V3 }
    private var v5: MQTTClient.V5 { client as! MQTTClient.V5 }

    var status: Status { client.status }

    func open(_ options: OpenOptions) async throws -> OpenResult {
        client.config.keepAlive = options.keepAlive
        client.config.pingEnabled = options.pingEnabled
        client.config.pingTimeout = options.pingTimeout
        client.config.publishTimeout = options.publishTimeout
        if let retry = options.retry {
            client.startRetrier(.equals(interval: retry.interval), limits: retry.limit)
        }
        let identity = Identity(options.clientId, username: options.username, password: options.password)
        switch version {
        case .v3_1_1:
            let sessionPresent = try await withDeadline("open") { try await v3.open(identity, cleanStart: options.cleanStart).wait() }
            return OpenResult(sessionPresent: sessionPresent, reasonCode: 0)
        case .v5_0:
            var authflow: Authflow?
            if let respond = options.authflow {
                authflow = { @Sendable (auth: Auth) -> Promise<Auth> in
                    Promise<Auth>(initializer: { @Sendable () async throws -> Auth in try await respond(auth) })
                }
            }
            let properties: Properties = options.authMethod.map { [.authenticationMethod($0)] } ?? []
            let connack = try await withDeadline("open") { [authflow] in
                try await v5.open(identity, cleanStart: options.cleanStart, properties: properties, authflow: authflow).wait()
            }
            return OpenResult(sessionPresent: connack.sessionPresent, reasonCode: connack.code.rawValue)
        }
    }

    func publish(_ topic: String, _ payload: String, qos: MQTTQoS, retain: Bool) async throws -> UInt8? {
        switch version {
        case .v3_1_1:
            try await withDeadline("publish") { try await v3.publish(to: topic, payload: payload, qos: qos, retain: retain).wait() }
            return qos == .atMostOnce ? nil : 0
        case .v5_0:
            return try await withDeadline("publish") {
                try await v5.publish(to: topic, payload: payload, qos: qos, retain: retain).wait()?.code.rawValue
            }
        }
    }

    func subscribe(_ topics: [String], qos: MQTTQoS) async throws -> [UInt8] {
        let suback = switch version {
        case .v3_1_1: try await withDeadline("subscribe") { try await v3.subscribe(to: topics.map { Subscribe.V3($0, qos: qos) }).wait() }
        case .v5_0: try await withDeadline("subscribe") { try await v5.subscribe(to: topics.map { Subscribe($0, qos: qos) }).wait() }
        }
        return suback.codes.map(\.rawValue)
    }

    func unsubscribe(_ topics: [String]) async throws -> [UInt8] {
        switch version {
        case .v3_1_1:
            try await withDeadline("unsubscribe") { try await v3.unsubscribe(from: topics).wait() }
            return []
        case .v5_0:
            return try await withDeadline("unsubscribe") { try await v5.unsubscribe(from: topics).wait().codes.map(\.rawValue) }
        }
    }

    func close() async throws {
        switch version {
        case .v3_1_1: try await withDeadline("close") { try await v3.close().wait() }
        case .v5_0: try await withDeadline("close") { try await v5.close().wait() }
        }
    }

    func shutdown() async {
        client.stopRetrier()
        client.stopMonitor()
        switch version {
        case .v3_1_1: v3.close()
        case .v5_0: v5.close()
        }
    }
}

/// Forwards legacy delegate callbacks into `ClientEvents`.
private final class LegacyDelegate: MQTTDelegate {
    let events: ClientEvents

    init(events: ClientEvents) { self.events = events }

    func mqtt(_ mqtt: MQTTClient, didUpdate status: Status, prev: Status) {
        events.record(status)
    }

    func mqtt(_ mqtt: MQTTClient, didReceive message: Message) {
        let received = ReceivedMessage(
            topic: message.topic,
            payload: String(decoding: message.payload, as: UTF8.self),
            qos: message.qos,
            retain: message.retain
        )
        events.record(received)
    }

    func mqtt(_ mqtt: MQTTClient, didReceive error: any Error) {}
}
