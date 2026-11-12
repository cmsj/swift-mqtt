import Foundation
import Testing
@testable import MQTT

struct PacketFramerTests {
    /// PINGRESP followed by a v5 PUBACK for packet 7.
    let twoPackets: [UInt8] = [0xD0, 0x00, 0x40, 0x02, 0x00, 0x07]

    @Test
    func emptyFramerHasNoPacket() throws {
        var framer = PacketFramer()
        #expect(try framer.next() == nil)
    }

    @Test
    func packetsArrivingByteByByte() throws {
        var framer = PacketFramer()
        var types: [PacketType] = []
        for byte in twoPackets {
            framer.append(Data([byte]))
            while let packet = try framer.next() {
                types.append(packet.type)
            }
        }
        #expect(types == [.PINGRESP, .PUBACK])
    }

    @Test
    func severalPacketsInOneChunk() throws {
        var framer = PacketFramer()
        framer.append(Data(twoPackets))
        #expect(try framer.next()?.type == .PINGRESP)
        let puback = try #require(try framer.next())
        #expect(try puback.packet(with: .v5_0).id == 7)
        #expect(try framer.next() == nil)
    }

    @Test
    func packetSplitInsideLengthField() throws {
        // PUBLISH with a 200-byte body: remaining length is two bytes, 0xC8 0x01.
        let body = [0x00, 0x01] + ascii("t") + [UInt8](repeating: 0x61, count: 197)
        let bytes: [UInt8] = [0x30, 0xC8, 0x01] + body
        var framer = PacketFramer()
        framer.append(Data(bytes[..<2]))
        #expect(try framer.next() == nil)
        framer.append(Data(bytes[2...]))
        let publish = try #require(try framer.next()?.packet(with: .v3_1_1) as? PublishPacket)
        #expect(publish.message.payload.count == 197)
    }

    @Test
    func unknownPacketTypeThrows() {
        var framer = PacketFramer()
        framer.append(Data([0x00, 0x00]))
        #expect(throws: MQTTError.decodeError(.unrecognisedPacketType)) {
            try framer.next()
        }
    }

    @Test
    func overlongLengthThrows() {
        var framer = PacketFramer()
        framer.append(Data([0x30, 0x80, 0x80, 0x80, 0x80, 0x01]))
        #expect(throws: MQTTError.decodeError(.varintOverflow)) {
            try framer.next()
        }
    }
}

extension IncomingPacket: Equatable {
    public static func == (lhs: IncomingPacket, rhs: IncomingPacket) -> Bool {
        lhs.type == rhs.type && lhs.flags == rhs.flags && lhs.remainingData == rhs.remainingData
    }
}
