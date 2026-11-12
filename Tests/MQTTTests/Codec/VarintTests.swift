import Foundation
import Testing
@testable import MQTT

struct VarintTests {
    /// Boundary values from MQTT 5.0 §1.5.5.
    @Test(arguments: [
        (0, [0x00]),
        (127, [0x7F]),
        (128, [0x80, 0x01]),
        (16_383, [0xFF, 0x7F]),
        (16_384, [0x80, 0x80, 0x01]),
        (2_097_151, [0xFF, 0xFF, 0x7F]),
        (2_097_152, [0x80, 0x80, 0x80, 0x01]),
        (268_435_455, [0xFF, 0xFF, 0xFF, 0x7F]),
    ] as [(Int, [UInt8])])
    func roundTripsBoundaryValues(value: Int, bytes: [UInt8]) throws {
        var buffer = DataBuffer()
        Serializer.writeVarint(value, to: &buffer)
        #expect(Array(buffer.data) == bytes)
        #expect(Serializer.varintPacketSize(value) == bytes.count)
        #expect(try Serializer.readVarint(from: &buffer) == value)
    }

    @Test
    func truncatedVarintIsIncomplete() {
        var buffer = DataBuffer(data: Data([0x80, 0x80]))
        #expect(throws: MQTTError.incompletePacket) {
            try Serializer.readVarint(from: &buffer)
        }
    }

    @Test
    func rejectsVarintLongerThanFourBytes() {
        var buffer = DataBuffer(data: Data([0x80, 0x80, 0x80, 0x80, 0x01]))
        #expect(throws: MQTTError.decodeError(.varintOverflow)) {
            try Serializer.readVarint(from: &buffer)
        }
    }
}
