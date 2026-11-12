import Foundation
import Testing
@testable import MQTT

extension Tag {
    /// Tests that need a real MQTT broker reachable over the network.
    @Tag static var integration: Self
}

/// Broker host for integration tests, from the environment.
private let integrationHost = ProcessInfo.processInfo.environment["MQTT_TEST_HOST"]

/// End-to-end tests against a real broker, for every transport.
///
/// Opt-in: set `MQTT_TEST_HOST` to a broker that serves the EMQX default ports
/// (1883 tcp, 8883 tls, 8083 ws, 8084 wss, 14567 quic), for example `broker.emqx.io`
/// or the cluster from `docker/docker-compose.yml`.
@Suite(
    .tags(.integration),
    .enabled(if: integrationHost != nil, "Set MQTT_TEST_HOST to run broker integration tests"),
    .timeLimit(.minutes(1))
)
struct BrokerIntegrationTests {
    enum Transport: String, CaseIterable, Sendable {
        case tcp, tls, ws, wss, quic

        func endpoint(host: String) -> Endpoint {
            switch self {
            case .tcp: .tcp(host: host)
            case .tls: .tls(host: host, tls: .trustAll())
            case .ws: .ws(host: host)
            case .wss: .wss(host: host, tls: .trustAll())
            case .quic: .quic(host: host, tls: .trustAll())
            }
        }
    }

    // Serialized: two simultaneous QUIC connections from one process get one reset
    // (ECONNRESET) by broker.emqx.io. To be investigated with the new transport.
    @Test(.serialized, arguments: Transport.allCases, [Version.v3_1_1, .v5_0])
    func publishedMessagesComeBack(transport: Transport, version: Version) async throws {
        let host = try #require(integrationHost)
        let client = makeClient(version: version, endpoint: transport.endpoint(host: host))
        do {
            try await roundTrip(client)
        } catch {
            await client.shutdown()
            throw error
        }
        await client.shutdown()
    }

    private func roundTrip(_ client: any ClientUnderTest) async throws {
        var options = OpenOptions()
        options.clientId = "swift-mqtt-test-\(UUID().uuidString.prefix(8))"
        _ = try await client.open(options)

        let topic = "swift-mqtt/test/\(UUID().uuidString)"
        let granted = try await client.subscribe([topic], qos: .exactlyOnce)
        #expect(granted.allSatisfy { $0 < 0x80 })

        let qosLevels: [MQTTQoS] = [.atMostOnce, .atLeastOnce, .exactlyOnce]
        for qos in qosLevels {
            let reason = try await client.publish(topic, "qos\(qos.rawValue)", qos: qos, retain: false)
            #expect(reason.map { $0 < 0x80 } ?? (qos == .atMostOnce))
        }

        var received: Set<String> = []
        for _ in qosLevels {
            received.insert(try await client.events.nextMessage(within: .seconds(10)).payload)
        }
        #expect(received == ["qos0", "qos1", "qos2"])

        _ = try await client.unsubscribe([topic])
        try await client.close()
    }
}
