import Foundation
import Testing
@testable import MQTT

@Suite(.timeLimit(.minutes(1)))
struct SubscribeTests {
    @Test(arguments: [
        (Version.v3_1_1, UInt8(0x01)),
        // v5 defaults: QoS 1, retain-as-published, retain handling "send if new".
        (Version.v5_0, UInt8(0x19)),
    ])
    func subscribeSendsFiltersAndReturnsCodes(version: Version, options: UInt8) async throws {
        try await withHarness(version) { h in
            let connection = try await h.connect()
            async let codes = h.client.subscribe(["a/#", "b"], qos: .atLeastOnce)
            let packet = try await connection.next()
            guard case .subscribe(let id, let filters) = packet else {
                Issue.record("expected SUBSCRIBE, got \(packet)")
                return
            }
            #expect(id != 0)
            #expect(filters == [.init(topic: "a/#", options: options), .init(topic: "b", options: options)])
            connection.send(BrokerEncoder.suback(id: id, codes: [0x01, 0x80], protocolLevel: h.protocolLevel))
            #expect(try await codes == [0x01, 0x80])
        }
    }

    @Test(arguments: [
        (Version.v3_1_1, [UInt8]()),
        (Version.v5_0, [UInt8(0x00), 0x11]),
    ])
    func unsubscribeSendsTopicsAndReturnsCodes(version: Version, codes expected: [UInt8]) async throws {
        try await withHarness(version) { h in
            let connection = try await h.connect()
            async let codes = h.client.unsubscribe(["a/#", "b"])
            let packet = try await connection.next()
            guard case .unsubscribe(let id, let topics) = packet else {
                Issue.record("expected UNSUBSCRIBE, got \(packet)")
                return
            }
            #expect(topics == ["a/#", "b"])
            connection.send(BrokerEncoder.unsuback(id: id, codes: [0x00, 0x11], protocolLevel: h.protocolLevel))
            #expect(try await codes == expected)
        }
    }

    @Test
    func emptySubscriptionIsRejected() async throws {
        try await withHarness(.v5_0) { h in
            _ = try await h.connect()
            await #expect(throws: MQTTError.packetError(.atLeastOneTopicRequired)) {
                try await h.client.subscribe([], qos: .atLeastOnce)
            }
            await #expect(throws: MQTTError.packetError(.atLeastOneTopicRequired)) {
                try await h.client.unsubscribe([])
            }
        }
    }
}
