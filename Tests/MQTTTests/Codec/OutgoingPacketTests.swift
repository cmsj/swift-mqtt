import Foundation
import Testing
@testable import MQTT

/// Golden-byte tests for every packet the client sends.
/// Expected bytes are derived by hand from the MQTT 3.1.1 and 5.0 specifications.
struct OutgoingPacketTests {
    static let cases: [EncodeCase] = [
        // MARK: CONNECT
        EncodeCase(
            name: "CONNECT clean session",
            version: .v3_1_1,
            packet: ConnectPacket(cleanSession: true, keepAlive: 60, clientId: "abc", username: nil, password: nil, properties: [], will: nil),
            expected: [0x10, 0x0F, 0x00, 0x04] + ascii("MQTT") + [0x04, 0x02, 0x00, 0x3C, 0x00, 0x03] + ascii("abc")
        ),
        EncodeCase(
            name: "CONNECT clean session",
            version: .v5_0,
            packet: ConnectPacket(cleanSession: true, keepAlive: 60, clientId: "abc", username: nil, password: nil, properties: [], will: nil),
            expected: [0x10, 0x10, 0x00, 0x04] + ascii("MQTT") + [0x05, 0x02, 0x00, 0x3C, 0x00, 0x00, 0x03] + ascii("abc")
        ),
        EncodeCase(
            name: "CONNECT credentials and session expiry",
            version: .v5_0,
            packet: ConnectPacket(
                cleanSession: false, keepAlive: 60, clientId: "abc", username: "u", password: "p",
                properties: [.sessionExpiryInterval(0xFFFF_FFFF)], will: nil
            ),
            expected: [0x10, 0x1B, 0x00, 0x04] + ascii("MQTT") + [0x05, 0xC0, 0x00, 0x3C]
                + [0x05, 0x11, 0xFF, 0xFF, 0xFF, 0xFF]
                + [0x00, 0x03] + ascii("abc") + [0x00, 0x01] + ascii("u") + [0x00, 0x01] + ascii("p")
        ),
        EncodeCase(
            name: "CONNECT with retained QoS 1 will",
            version: .v3_1_1,
            packet: ConnectPacket(
                cleanSession: true, keepAlive: 60, clientId: "abc", username: nil, password: nil,
                properties: [], will: message("w", "x", qos: .atLeastOnce, retain: true)
            ),
            expected: [0x10, 0x15, 0x00, 0x04] + ascii("MQTT") + [0x04, 0x2E, 0x00, 0x3C]
                + [0x00, 0x03] + ascii("abc") + [0x00, 0x01] + ascii("w") + [0x00, 0x01] + ascii("x")
        ),
        EncodeCase(
            name: "CONNECT with will",
            version: .v5_0,
            packet: ConnectPacket(
                cleanSession: true, keepAlive: 60, clientId: "abc", username: nil, password: nil,
                properties: [], will: message("w", "x")
            ),
            expected: [0x10, 0x17, 0x00, 0x04] + ascii("MQTT") + [0x05, 0x06, 0x00, 0x3C, 0x00]
                + [0x00, 0x03] + ascii("abc") + [0x00] + [0x00, 0x01] + ascii("w") + [0x00, 0x01] + ascii("x")
        ),

        // MARK: PUBLISH
        EncodeCase(
            name: "PUBLISH QoS 0",
            version: .v3_1_1,
            packet: PublishPacket(id: 0, message: message("a/b", "hi")),
            expected: [0x30, 0x07, 0x00, 0x03] + ascii("a/b") + ascii("hi")
        ),
        EncodeCase(
            name: "PUBLISH QoS 1 dup retain",
            version: .v3_1_1,
            packet: PublishPacket(id: 10, message: message("a/b", "hi", qos: .atLeastOnce, dup: true, retain: true)),
            expected: [0x3B, 0x09, 0x00, 0x03] + ascii("a/b") + [0x00, 0x0A] + ascii("hi")
        ),
        EncodeCase(
            name: "PUBLISH QoS 0",
            version: .v5_0,
            packet: PublishPacket(id: 0, message: message("a/b", "hi")),
            expected: [0x30, 0x08, 0x00, 0x03] + ascii("a/b") + [0x00] + ascii("hi")
        ),
        EncodeCase(
            name: "PUBLISH QoS 2 with content type",
            version: .v5_0,
            packet: PublishPacket(id: 0x0102, message: message("a/b", "hi", qos: .exactlyOnce, properties: [.contentType("t")])),
            expected: [0x34, 0x0E, 0x00, 0x03] + ascii("a/b") + [0x01, 0x02] + [0x04, 0x03, 0x00, 0x01] + ascii("t") + ascii("hi")
        ),

        // MARK: SUBSCRIBE / UNSUBSCRIBE
        EncodeCase(
            name: "SUBSCRIBE two topics",
            version: .v3_1_1,
            packet: SubscribePacket(id: 1, properties: [], subscriptions: [.init("a", qos: .atLeastOnce), .init("b/#", qos: .exactlyOnce)]),
            expected: [0x82, 0x0C, 0x00, 0x01, 0x00, 0x01] + ascii("a") + [0x01, 0x00, 0x03] + ascii("b/#") + [0x02]
        ),
        EncodeCase(
            name: "SUBSCRIBE default options",
            version: .v5_0,
            packet: SubscribePacket(id: 2, properties: [], subscriptions: [.init("a", qos: .atLeastOnce)]),
            expected: [0x82, 0x07, 0x00, 0x02, 0x00, 0x00, 0x01] + ascii("a") + [0x19]
        ),
        EncodeCase(
            name: "SUBSCRIBE all options",
            version: .v5_0,
            packet: SubscribePacket(
                id: 2, properties: [],
                subscriptions: [.init("a", qos: .atLeastOnce, noLocal: true, retainAsPublished: true, retainHandling: .doNotSend)]
            ),
            expected: [0x82, 0x07, 0x00, 0x02, 0x00, 0x00, 0x01] + ascii("a") + [0x2D]
        ),
        EncodeCase(
            name: "UNSUBSCRIBE two topics",
            version: .v3_1_1,
            packet: UnsubscribePacket(id: 3, subscriptions: ["a", "bc"], properties: []),
            expected: [0xA2, 0x09, 0x00, 0x03, 0x00, 0x01] + ascii("a") + [0x00, 0x02] + ascii("bc")
        ),
        EncodeCase(
            name: "UNSUBSCRIBE",
            version: .v5_0,
            packet: UnsubscribePacket(id: 3, subscriptions: ["a"], properties: []),
            expected: [0xA2, 0x06, 0x00, 0x03, 0x00, 0x00, 0x01] + ascii("a")
        ),

        // MARK: Acknowledgements
        EncodeCase(name: "PUBACK", version: .v3_1_1, packet: PubackPacket(id: 5, type: .PUBACK), expected: [0x40, 0x02, 0x00, 0x05]),
        EncodeCase(name: "PUBACK success short form", version: .v5_0, packet: PubackPacket(id: 5, type: .PUBACK), expected: [0x40, 0x02, 0x00, 0x05]),
        EncodeCase(
            name: "PUBACK with reason code",
            version: .v5_0,
            packet: PubackPacket(id: 5, type: .PUBACK, code: .noMatchingSubscribers),
            expected: [0x40, 0x04, 0x00, 0x05, 0x10, 0x00]
        ),
        EncodeCase(name: "PUBREC", version: .v5_0, packet: PubackPacket(id: 5, type: .PUBREC), expected: [0x50, 0x02, 0x00, 0x05]),
        EncodeCase(name: "PUBREL", version: .v3_1_1, packet: PubackPacket(id: 5, type: .PUBREL), expected: [0x62, 0x02, 0x00, 0x05]),
        EncodeCase(name: "PUBCOMP", version: .v5_0, packet: PubackPacket(id: 5, type: .PUBCOMP), expected: [0x70, 0x02, 0x00, 0x05]),

        // MARK: Control packets
        EncodeCase(name: "PINGREQ", version: .v3_1_1, packet: PingreqPacket(), expected: [0xC0, 0x00]),
        EncodeCase(name: "PINGREQ", version: .v5_0, packet: PingreqPacket(), expected: [0xC0, 0x00]),
        EncodeCase(name: "DISCONNECT", version: .v3_1_1, packet: DisconnectPacket(code: .normal), expected: [0xE0, 0x00]),
        EncodeCase(name: "DISCONNECT normal short form", version: .v5_0, packet: DisconnectPacket(code: .normal), expected: [0xE0, 0x00]),
        EncodeCase(
            name: "DISCONNECT with will",
            version: .v5_0,
            packet: DisconnectPacket(code: .disconnectWithWill),
            expected: [0xE0, 0x02, 0x04, 0x00]
        ),
        EncodeCase(
            name: "AUTH re-authenticate",
            version: .v5_0,
            packet: AuthPacket(code: .reAuthenticate, properties: [.authenticationMethod("m")]),
            expected: [0xF0, 0x06, 0x19, 0x04, 0x15, 0x00, 0x01] + ascii("m")
        ),
    ]

    @Test(arguments: cases)
    func encodesToSpecBytes(_ testCase: EncodeCase) throws {
        let bytes = try encode(testCase.packet, testCase.version)
        #expect(bytes == testCase.expected)
    }

    @Test
    func rejectsStringsLongerThan65535Bytes() {
        let topic = String(repeating: "a", count: 65_536)
        let packet = PublishPacket(id: 0, message: message(topic, ""))
        #expect(throws: MQTTError.packetError(.badParameter)) {
            try encode(packet, .v5_0)
        }
    }
}
