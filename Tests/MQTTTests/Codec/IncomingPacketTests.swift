import Foundation
import Testing
@testable import MQTT

/// Golden-byte tests for every packet the client receives from a broker.
struct IncomingPacketTests {
    // MARK: CONNACK

    @Test
    func connackV3SessionPresent() throws {
        let connack = try #require(try decode([0x20, 0x02, 0x01, 0x00], .v3_1_1) as? ConnackPacket)
        #expect(connack.sessionPresent)
        #expect(connack.returnCode == 0)
    }

    @Test
    func connackV3Refused() throws {
        let connack = try #require(try decode([0x20, 0x02, 0x00, 0x05], .v3_1_1) as? ConnackPacket)
        #expect(connack.sessionPresent == false)
        #expect(connack.returnCode == ResultCode.ConnectV3.notAuthorized.rawValue)
    }

    @Test
    func connackV5WithoutProperties() throws {
        let connack = try #require(try decode([0x20, 0x03, 0x00, 0x00, 0x00], .v5_0) as? ConnackPacket)
        #expect(connack.returnCode == 0)
        #expect(connack.properties.isEmpty)
    }

    @Test
    func connackV5WithProperties() throws {
        let bytes: [UInt8] = [0x20, 0x0A, 0x00, 0x00, 0x07, 0x24, 0x01, 0x12, 0x00, 0x02] + ascii("id")
        let connack = try #require(try decode(bytes, .v5_0) as? ConnackPacket)
        #expect(connack.properties == [.maximumQoS(.atLeastOnce), .assignedClientIdentifier("id")])
    }

    // MARK: PUBLISH

    @Test
    func publishV3RetainedQoS0() throws {
        let publish = try #require(try decode([0x31, 0x04, 0x00, 0x01] + ascii("t") + ascii("x"), .v3_1_1) as? PublishPacket)
        #expect(publish.id == 0)
        #expect(publish.message.qos == .atMostOnce)
        #expect(publish.message.retain)
        #expect(publish.message.dup == false)
        #expect(publish.message.topic == "t")
        #expect(publish.message.payload == Data("x".utf8))
    }

    @Test
    func publishV5QoS1WithUserProperty() throws {
        let bytes: [UInt8] = [0x32, 0x0E, 0x00, 0x01] + ascii("t") + [0x00, 0x07]
            + [0x07, 0x26, 0x00, 0x01] + ascii("k") + [0x00, 0x01] + ascii("v") + ascii("x")
        let publish = try #require(try decode(bytes, .v5_0) as? PublishPacket)
        #expect(publish.id == 7)
        #expect(publish.message.qos == .atLeastOnce)
        #expect(publish.message.properties == [.userProperty("k", "v")])
        #expect(publish.message.payload == Data("x".utf8))
    }

    @Test
    func publishV5DuplicateQoS2() throws {
        let bytes: [UInt8] = [0x3C, 0x06, 0x00, 0x01] + ascii("t") + [0x00, 0x09, 0x00]
        let publish = try #require(try decode(bytes, .v5_0) as? PublishPacket)
        #expect(publish.id == 9)
        #expect(publish.message.qos == .exactlyOnce)
        #expect(publish.message.dup)
        #expect(publish.message.payload.isEmpty)
    }

    // MARK: Acknowledgements

    @Test(arguments: [
        (UInt8(0x40), PacketType.PUBACK),
        (UInt8(0x50), PacketType.PUBREC),
        (UInt8(0x62), PacketType.PUBREL),
        (UInt8(0x70), PacketType.PUBCOMP),
    ])
    func acknowledgementShortForm(header: UInt8, type: PacketType) throws {
        let ack = try #require(try decode([header, 0x02, 0x00, 0x07], .v5_0) as? PubackPacket)
        #expect(ack.type == type)
        #expect(ack.id == 7)
        #expect(ack.code == .success)
    }

    @Test
    func pubackV5ReasonCodeWithoutProperties() throws {
        let ack = try #require(try decode([0x40, 0x03, 0x00, 0x07, 0x87], .v5_0) as? PubackPacket)
        #expect(ack.code == .notAuthorized)
        #expect(ack.properties.isEmpty)
    }

    @Test
    func pubackV5ReasonString() throws {
        let ack = try #require(try decode([0x40, 0x08, 0x00, 0x07, 0x87, 0x04, 0x1F, 0x00, 0x01] + ascii("r"), .v5_0) as? PubackPacket)
        #expect(ack.code == .notAuthorized)
        #expect(ack.properties == [.reasonString("r")])
    }

    @Test
    func subackV3() throws {
        let suback = try #require(try decode([0x90, 0x04, 0x00, 0x01, 0x01, 0x80], .v3_1_1) as? SubackPacket)
        #expect(suback.type == .SUBACK)
        #expect(suback.id == 1)
        #expect(suback.suback().codes == [.grantedQoS1, .unspecifiedError])
    }

    @Test
    func subackV5() throws {
        let suback = try #require(try decode([0x90, 0x05, 0x00, 0x01, 0x00, 0x00, 0x02], .v5_0) as? SubackPacket)
        #expect(suback.suback().codes == [.grantedQoS0, .grantedQoS2])
        #expect(suback.properties.isEmpty)
    }

    @Test
    func unsubackV3HasNoCodes() throws {
        let unsuback = try #require(try decode([0xB0, 0x02, 0x00, 0x01], .v3_1_1) as? SubackPacket)
        #expect(unsuback.type == .UNSUBACK)
        #expect(unsuback.unsuback().codes.isEmpty)
    }

    @Test
    func unsubackV5() throws {
        let unsuback = try #require(try decode([0xB0, 0x05, 0x00, 0x01, 0x00, 0x00, 0x11], .v5_0) as? SubackPacket)
        #expect(unsuback.unsuback().codes == [.success, .noSubscriptionExisted])
    }

    // MARK: Control packets

    @Test
    func pingresp() throws {
        #expect(try decode([0xD0, 0x00], .v5_0) is PingrespPacket)
    }

    @Test
    func disconnectV5Empty() throws {
        let disconnect = try #require(try decode([0xE0, 0x00], .v5_0) as? DisconnectPacket)
        #expect(disconnect.code == .normal)
    }

    @Test
    func disconnectV5ReasonCodeOnly() throws {
        let disconnect = try #require(try decode([0xE0, 0x01, 0x8B], .v5_0) as? DisconnectPacket)
        #expect(disconnect.code == .serverShuttingDown)
        #expect(disconnect.properties.isEmpty)
    }

    @Test
    func disconnectV5ReasonString() throws {
        let disconnect = try #require(try decode([0xE0, 0x06, 0x8B, 0x04, 0x1F, 0x00, 0x01] + ascii("r"), .v5_0) as? DisconnectPacket)
        #expect(disconnect.code == .serverShuttingDown)
        #expect(disconnect.properties == [.reasonString("r")])
    }

    @Test
    func authEmptyMeansSuccess() throws {
        let auth = try #require(try decode([0xF0, 0x00], .v5_0) as? AuthPacket)
        #expect(auth.code == .success)
    }

    @Test
    func authContinue() throws {
        let auth = try #require(try decode([0xF0, 0x02, 0x18, 0x00], .v5_0) as? AuthPacket)
        #expect(auth.code == .continueAuthentication)
    }

    // MARK: Malformed input

    @Test
    func truncatedPacketIsIncomplete() {
        #expect(throws: MQTTError.incompletePacket) {
            try decode([0x30, 0x05, 0x00, 0x09] + ascii("t"), .v5_0)
        }
    }

    @Test
    func unknownPacketType() {
        #expect(throws: MQTTError.decodeError(.unrecognisedPacketType)) {
            try decode([0x00, 0x00], .v5_0)
        }
    }

    @Test
    func connackTooShort() {
        #expect(throws: MQTTError.decodeError(.unexpectedTokens)) {
            try decode([0x20, 0x01, 0x00], .v5_0)
        }
    }

    @Test
    func clientOnlyPacketsAreRejected() {
        #expect(throws: MQTTError.unexpectPacket) {
            try decode([0x82, 0x02, 0x00, 0x01], .v5_0)
        }
    }
}
