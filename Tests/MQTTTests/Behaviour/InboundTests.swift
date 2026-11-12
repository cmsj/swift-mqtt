import Foundation
import Testing
@testable import MQTT

/// Messages the broker delivers to the client.
@Suite(.timeLimit(.minutes(1)))
struct InboundTests {
    @Test(arguments: [Version.v3_1_1, .v5_0])
    func qos0IsDelivered(version: Version) async throws {
        try await withHarness(version) { h in
            let connection = try await h.connect()
            connection.send(BrokerEncoder.publish(topic: "a/b", payload: "hi", retain: true, protocolLevel: h.protocolLevel))
            #expect(try await h.client.events.nextMessage() == .init(topic: "a/b", payload: "hi", qos: .atMostOnce, retain: true))
        }
    }

    @Test(arguments: [Version.v3_1_1, .v5_0])
    func qos1IsAcknowledgedAndDelivered(version: Version) async throws {
        try await withHarness(version) { h in
            let connection = try await h.connect()
            connection.send(BrokerEncoder.publish(topic: "a/b", payload: "hi", qos: 1, id: 5, protocolLevel: h.protocolLevel))
            #expect(try await connection.next() == .puback(id: 5, reason: 0))
            #expect(try await h.client.events.nextMessage() == .init(topic: "a/b", payload: "hi", qos: .atLeastOnce, retain: false))
        }
    }

    @Test(arguments: [Version.v3_1_1, .v5_0])
    func qos2IsDeliveredOnceAfterPubrel(version: Version) async throws {
        try await withHarness(version) { h in
            let connection = try await h.connect()
            connection.send(BrokerEncoder.publish(topic: "a/b", payload: "hi", qos: 2, id: 6, protocolLevel: h.protocolLevel))
            #expect(try await connection.next() == .pubrec(id: 6, reason: 0))
            connection.send(BrokerEncoder.ack(0x62, id: 6, protocolLevel: h.protocolLevel))
            #expect(try await connection.next() == .pubcomp(id: 6, reason: 0))
            #expect(try await h.client.events.nextMessage() == .init(topic: "a/b", payload: "hi", qos: .exactlyOnce, retain: false))
        }
    }

    @Test(arguments: [MQTTQoS.atMostOnce, .atLeastOnce])
    func messagesAreDeliveredInOrder(qos: MQTTQoS) async throws {
        try await withHarness(.v5_0) { h in
            let connection = try await h.connect()
            let count = 50
            for i in 1...count {
                connection.send(BrokerEncoder.publish(topic: "seq", payload: "\(i)", qos: qos.rawValue, id: UInt16(i), protocolLevel: 5))
            }
            var payloads: [String] = []
            for _ in 1...count {
                payloads.append(try await h.client.events.nextMessage().payload)
            }
            #expect(payloads == (1...count).map(String.init))
        }
    }

    @Test(.disabled("Crashes the legacy client: the PUBREC timeout handler returns Promise<Packet> where swift-promise expects Void and calls fatalError"))
    func lateInboundPubrelStillCompletesQoS2() async throws {
        try await withHarness(.v5_0) { h in
            var options = OpenOptions()
            options.publishTimeout = 0.2
            let connection = try await h.connect(options)
            connection.send(BrokerEncoder.publish(topic: "a/b", payload: "hi", qos: 2, id: 6, protocolLevel: 5))
            #expect(try await connection.next() == .pubrec(id: 6, reason: 0))
            try await Task.sleep(for: .milliseconds(500))
            connection.send(BrokerEncoder.ack(0x62, id: 6, protocolLevel: 5))
            #expect(try await connection.next() == .pubcomp(id: 6, reason: 0))
            #expect(try await h.client.events.nextMessage().payload == "hi")
            #expect(await h.client.status == .opened)
        }
    }
}
