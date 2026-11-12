import Foundation
import Testing
@testable import MQTT

/// Outbound PUBLISH flows: the client publishes, the fake broker acknowledges (or doesn't).
@Suite(.timeLimit(.minutes(1)))
struct PublishTests {
    @Test(arguments: [Version.v3_1_1, .v5_0])
    func qos0IsSentWithoutPacketId(version: Version) async throws {
        try await withHarness(version) { h in
            let connection = try await h.connect()
            let result = try await h.client.publish("a/b", "hello", qos: .atMostOnce, retain: true)
            let publish = try await connection.expectPublish()
            #expect(result == nil)
            #expect(publish == .init(id: 0, qos: 0, dup: false, retain: true, topic: "a/b", payload: Data("hello".utf8)))
        }
    }

    @Test(arguments: [Version.v3_1_1, .v5_0])
    func qos1CompletesOnPuback(version: Version) async throws {
        try await withHarness(version) { h in
            let connection = try await h.connect()
            async let result = h.client.publish("a/b", "hello", qos: .atLeastOnce)
            let publish = try await connection.expectPublish()
            #expect(publish.qos == 1)
            #expect(publish.id != 0)
            #expect(publish.dup == false)
            connection.send(BrokerEncoder.ack(0x40, id: publish.id, protocolLevel: h.protocolLevel))
            #expect(try await result == 0)
        }
    }

    @Test
    func qos1RejectedByBrokerThrows() async throws {
        try await withHarness(.v5_0) { h in
            let connection = try await h.connect()
            let publishing = Task { try await h.client.publish("a/b", "hello", qos: .atLeastOnce) }
            let publish = try await connection.expectPublish()
            connection.send(BrokerEncoder.ack(0x40, id: publish.id, reason: 0x87, protocolLevel: 5))
            await #expect(throws: MQTTError.publishFailed(.notAuthorized)) {
                try await publishing.value
            }
        }
    }

    @Test(arguments: [Version.v3_1_1, .v5_0])
    func qos2CompletesAfterFourWayHandshake(version: Version) async throws {
        try await withHarness(version) { h in
            let connection = try await h.connect()
            async let result = h.client.publish("a/b", "hello", qos: .exactlyOnce)
            let publish = try await connection.expectPublish()
            #expect(publish.qos == 2)
            connection.send(BrokerEncoder.ack(0x50, id: publish.id, protocolLevel: h.protocolLevel))
            #expect(try await connection.next() == .pubrel(id: publish.id, reason: 0))
            connection.send(BrokerEncoder.ack(0x70, id: publish.id, protocolLevel: h.protocolLevel))
            #expect(try await result == 0)
        }
    }

    @Test
    func packetIdsIncrementPerRequest() async throws {
        try await withHarness(.v5_0) { h in
            let connection = try await h.connect()
            var ids: [UInt16] = []
            for _ in 0..<3 {
                async let result = h.client.publish("a/b", "x", qos: .atLeastOnce)
                let publish = try await connection.expectPublish()
                ids.append(publish.id)
                connection.send(BrokerEncoder.ack(0x40, id: publish.id, protocolLevel: 5))
                _ = try await result
            }
            #expect(ids == [ids[0], ids[0] + 1, ids[0] + 2])
        }
    }

    @Test(arguments: ["a/#", "a/+/b"])
    func wildcardTopicIsRejected(topic: String) async throws {
        try await withHarness(.v5_0) { h in
            _ = try await h.connect()
            await #expect(throws: MQTTError.packetError(.invalidTopicName)) {
                try await h.client.publish(topic, "x", qos: .atMostOnce)
            }
        }
    }

    @Test
    func qosAboveServerMaximumIsRejected() async throws {
        try await withHarness(.v5_0) { h in
            _ = try await h.connect(properties: [BrokerEncoder.maximumQoS(0)])
            await #expect(throws: MQTTError.packetError(.qosInvalid)) {
                try await h.client.publish("a/b", "x", qos: .atLeastOnce)
            }
        }
    }

    /// MQTT 5 §4.4 only allows resending on reconnect, so a timed-out publish fails
    /// and stays in the session instead of being resent on the same connection.
    @Test
    func unacknowledgedQoS1TimesOutWithoutResending() async throws {
        try await withHarness(.v5_0) { h in
            var options = OpenOptions()
            options.requestTimeout = .milliseconds(200)
            let connection = try await h.connect(options)
            let publishing = Task { try await h.client.publish("a/b", "hello", qos: .atLeastOnce) }
            _ = try await connection.expectPublish()
            await #expect(throws: MQTTError.timeout) {
                try await publishing.value
            }
            await #expect(throws: MailboxError.self) {
                try await connection.next(within: .milliseconds(300))
            }
            #expect(await h.client.status == .opened)
        }
    }

    /// The legacy client crashed here: its PUBREL timeout handler hit a fatalError in swift-promise.
    @Test
    func unacknowledgedPubrelTimesOut() async throws {
        try await withHarness(.v5_0) { h in
            var options = OpenOptions()
            options.requestTimeout = .milliseconds(200)
            let connection = try await h.connect(options)
            let publishing = Task { try await h.client.publish("a/b", "hello", qos: .exactlyOnce) }
            let publish = try await connection.expectPublish()
            connection.send(BrokerEncoder.ack(0x50, id: publish.id, protocolLevel: 5))
            #expect(try await connection.next() == .pubrel(id: publish.id, reason: 0))
            await #expect(throws: MQTTError.timeout) {
                try await publishing.value
            }
            #expect(await h.client.status == .opened)
        }
    }
}
