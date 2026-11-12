import Foundation
import Testing
@testable import MQTT

/// Encodes a client packet exactly as the client would put it on the wire.
func encode(_ packet: some Packet, _ version: Version) throws -> [UInt8] {
    var buffer = DataBuffer()
    try packet.write(version: version, to: &buffer)
    return Array(buffer.data)
}

/// Decodes one complete packet from raw bytes, as received from a broker.
func decode(_ bytes: [UInt8], _ version: Version) throws -> any Packet {
    var buffer = DataBuffer(data: Data(bytes))
    return try IncomingPacket.read(from: &buffer).packet(with: version)
}

/// UTF-8 bytes of an ASCII string, for building expected wire bytes readably.
func ascii(_ string: String) -> [UInt8] {
    Array(string.utf8)
}

func message(
    _ topic: String,
    _ payload: String,
    qos: MQTTQoS = .atMostOnce,
    dup: Bool = false,
    retain: Bool = false,
    properties: Properties = []
) -> Message {
    Message(qos: qos, dup: dup, topic: topic, retain: retain, payload: Data(payload.utf8), properties: properties)
}

/// A named encode case, so parameterized test output says which packet failed.
struct EncodeCase: Sendable, CustomTestStringConvertible {
    let name: String
    let version: Version
    let packet: any Packet
    let expected: [UInt8]
    var testDescription: String { "\(name) (\(version.string))" }
}
