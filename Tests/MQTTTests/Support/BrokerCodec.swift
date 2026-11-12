import Foundation

// A deliberately independent, broker-side MQTT codec. It shares no code with the
// library, so the behaviour tests check the client against a second implementation
// of the spec rather than against itself.

/// A packet sent by the client, as decoded by the fake broker.
enum ClientPacket: Sendable, Equatable {
    case connect(Connect)
    case publish(Publish)
    case puback(id: UInt16, reason: UInt8)
    case pubrec(id: UInt16, reason: UInt8)
    case pubrel(id: UInt16, reason: UInt8)
    case pubcomp(id: UInt16, reason: UInt8)
    case subscribe(id: UInt16, filters: [Filter])
    case unsubscribe(id: UInt16, topics: [String])
    case pingreq
    case disconnect(reason: UInt8)
    case auth(reason: UInt8, method: String?)

    struct Connect: Sendable, Equatable {
        var protocolLevel: UInt8
        var cleanStart: Bool
        var keepAlive: UInt16
        var clientId: String
        var username: String?
        var password: String?
        var willTopic: String?
        var sessionExpiry: UInt32?
        var authenticationMethod: String?
    }

    struct Publish: Sendable, Equatable {
        var id: UInt16
        var qos: UInt8
        var dup: Bool
        var retain: Bool
        var topic: String
        var payload: Data
    }

    struct Filter: Sendable, Equatable {
        var topic: String
        var options: UInt8
    }
}

enum BrokerCodecError: Error {
    case truncated
    case unknownPacketType(UInt8)
}

/// Sequential reader over a packet body.
struct BodyReader {
    private let bytes: [UInt8]
    private var index = 0
    init(_ bytes: [UInt8]) { self.bytes = bytes }

    var remaining: Int { bytes.count - index }

    mutating func byte() throws -> UInt8 {
        guard remaining >= 1 else { throw BrokerCodecError.truncated }
        defer { index += 1 }
        return bytes[index]
    }

    mutating func uint16() throws -> UInt16 {
        UInt16(try byte()) << 8 | UInt16(try byte())
    }

    mutating func uint32() throws -> UInt32 {
        var value: UInt32 = 0
        for _ in 0..<4 { value = value << 8 | UInt32(try byte()) }
        return value
    }

    mutating func varint() throws -> Int {
        var value = 0
        var shift = 0
        while true {
            let byte = try byte()
            value |= Int(byte & 0x7F) << shift
            if byte & 0x80 == 0 { return value }
            shift += 7
        }
    }

    mutating func data(count: Int) throws -> [UInt8] {
        guard remaining >= count else { throw BrokerCodecError.truncated }
        defer { index += count }
        return Array(bytes[index..<index + count])
    }

    mutating func binary() throws -> [UInt8] {
        try data(count: Int(try uint16()))
    }

    mutating func string() throws -> String {
        String(decoding: try binary(), as: UTF8.self)
    }

    mutating func rest() throws -> [UInt8] {
        try data(count: remaining)
    }

    /// Reads a v5 property block, returning the properties we care about and skipping the rest.
    mutating func properties() throws -> [UInt8: [UInt8]] {
        var properties: [UInt8: [UInt8]] = [:]
        var block = BodyReader(try data(count: try varint()))
        while block.remaining > 0 {
            let id = try block.byte()
            switch id {
            case 0x01, 0x17, 0x19, 0x24, 0x25, 0x28, 0x29, 0x2A: properties[id] = try block.data(count: 1)
            case 0x13, 0x21, 0x22, 0x23: properties[id] = try block.data(count: 2)
            case 0x02, 0x11, 0x18, 0x27: properties[id] = try block.data(count: 4)
            case 0x0B: properties[id] = BrokerEncoder.varint(try block.varint())
            case 0x03, 0x08, 0x12, 0x15, 0x1A, 0x1C, 0x1F, 0x09, 0x16: properties[id] = try block.binary()
            case 0x26: _ = try block.binary(); _ = try block.binary()
            default: throw BrokerCodecError.unknownPacketType(id)
            }
        }
        return properties
    }
}

enum BrokerDecoder {
    /// Decodes a client packet. `protocolLevel` is 4 for v3.1.1 and 5 for v5.
    static func decode(header: UInt8, body: [UInt8], protocolLevel: UInt8) throws -> ClientPacket {
        var reader = BodyReader(body)
        let isV5 = protocolLevel == 5
        func ackFields() throws -> (UInt16, UInt8) {
            let id = try reader.uint16()
            let reason = isV5 && reader.remaining > 0 ? try reader.byte() : 0
            return (id, reason)
        }
        switch header >> 4 {
        case 1:
            return .connect(try decodeConnect(&reader))
        case 3:
            let qos = (header >> 1) & 0x03
            let topic = try reader.string()
            let id = qos > 0 ? try reader.uint16() : 0
            if isV5 { _ = try reader.properties() }
            return .publish(.init(
                id: id, qos: qos, dup: header & 0x08 != 0, retain: header & 0x01 != 0,
                topic: topic, payload: Data(try reader.rest())
            ))
        case 4:
            let (id, reason) = try ackFields()
            return .puback(id: id, reason: reason)
        case 5:
            let (id, reason) = try ackFields()
            return .pubrec(id: id, reason: reason)
        case 6:
            let (id, reason) = try ackFields()
            return .pubrel(id: id, reason: reason)
        case 7:
            let (id, reason) = try ackFields()
            return .pubcomp(id: id, reason: reason)
        case 8:
            let id = try reader.uint16()
            if isV5 { _ = try reader.properties() }
            var filters: [ClientPacket.Filter] = []
            while reader.remaining > 0 {
                filters.append(.init(topic: try reader.string(), options: try reader.byte()))
            }
            return .subscribe(id: id, filters: filters)
        case 10:
            let id = try reader.uint16()
            if isV5 { _ = try reader.properties() }
            var topics: [String] = []
            while reader.remaining > 0 { topics.append(try reader.string()) }
            return .unsubscribe(id: id, topics: topics)
        case 12:
            return .pingreq
        case 14:
            return .disconnect(reason: isV5 && reader.remaining > 0 ? try reader.byte() : 0)
        case 15:
            guard reader.remaining > 0 else { return .auth(reason: 0, method: nil) }
            let reason = try reader.byte()
            let properties = try reader.properties()
            return .auth(reason: reason, method: properties[0x15].map { String(decoding: $0, as: UTF8.self) })
        default:
            throw BrokerCodecError.unknownPacketType(header)
        }
    }

    private static func decodeConnect(_ reader: inout BodyReader) throws -> ClientPacket.Connect {
        _ = try reader.string() // "MQTT"
        let level = try reader.byte()
        let flags = try reader.byte()
        let keepAlive = try reader.uint16()
        var properties: [UInt8: [UInt8]] = [:]
        if level == 5 { properties = try reader.properties() }
        var connect = ClientPacket.Connect(
            protocolLevel: level, cleanStart: flags & 0x02 != 0, keepAlive: keepAlive, clientId: try reader.string()
        )
        connect.sessionExpiry = properties[0x11].map { $0.reduce(0) { $0 << 8 | UInt32($1) } }
        connect.authenticationMethod = properties[0x15].map { String(decoding: $0, as: UTF8.self) }
        if flags & 0x04 != 0 {
            if level == 5 { _ = try reader.properties() }
            connect.willTopic = try reader.string()
            _ = try reader.binary()
        }
        if flags & 0x80 != 0 { connect.username = try reader.string() }
        if flags & 0x40 != 0 { connect.password = try reader.string() }
        return connect
    }
}

/// Encodes broker-to-client packets.
enum BrokerEncoder {
    static func varint(_ value: Int) -> [UInt8] {
        var value = value
        var bytes: [UInt8] = []
        repeat {
            var byte = UInt8(value & 0x7F)
            value >>= 7
            if value > 0 { byte |= 0x80 }
            bytes.append(byte)
        } while value > 0
        return bytes
    }

    static func uint16(_ value: UInt16) -> [UInt8] { [UInt8(value >> 8), UInt8(value & 0xFF)] }

    static func string(_ value: String) -> [UInt8] {
        let utf8 = Array(value.utf8)
        return uint16(UInt16(utf8.count)) + utf8
    }

    static func frame(_ header: UInt8, _ body: [UInt8]) -> Data {
        Data([header] + varint(body.count) + body)
    }

    /// A v5 property block. `properties` are already-encoded `id + value` byte runs.
    static func properties(_ properties: [[UInt8]]) -> [UInt8] {
        let flat = properties.flatMap { $0 }
        return varint(flat.count) + flat
    }

    static func connack(sessionPresent: Bool = false, reason: UInt8 = 0, protocolLevel: UInt8, properties: [[UInt8]] = []) -> Data {
        var body: [UInt8] = [sessionPresent ? 1 : 0, reason]
        if protocolLevel == 5 { body += self.properties(properties) }
        return frame(0x20, body)
    }

    static func publish(
        topic: String, payload: String, qos: UInt8 = 0, id: UInt16 = 0,
        dup: Bool = false, retain: Bool = false, protocolLevel: UInt8
    ) -> Data {
        var header: UInt8 = 0x30 | qos << 1
        if dup { header |= 0x08 }
        if retain { header |= 0x01 }
        var body = string(topic)
        if qos > 0 { body += uint16(id) }
        if protocolLevel == 5 { body += [0x00] }
        body += Array(payload.utf8)
        return frame(header, body)
    }

    /// PUBACK (0x40), PUBREC (0x50), PUBREL (0x62) or PUBCOMP (0x70).
    static func ack(_ header: UInt8, id: UInt16, reason: UInt8 = 0, protocolLevel: UInt8) -> Data {
        var body = uint16(id)
        if protocolLevel == 5, reason != 0 { body += [reason, 0x00] }
        return frame(header, body)
    }

    static func suback(id: UInt16, codes: [UInt8], protocolLevel: UInt8) -> Data {
        frame(0x90, uint16(id) + (protocolLevel == 5 ? [0x00] : []) + codes)
    }

    static func unsuback(id: UInt16, codes: [UInt8], protocolLevel: UInt8) -> Data {
        protocolLevel == 5 ? frame(0xB0, uint16(id) + [0x00] + codes) : frame(0xB0, uint16(id))
    }

    static let pingresp = Data([0xD0, 0x00])

    static func disconnect(reason: UInt8, protocolLevel: UInt8) -> Data {
        protocolLevel == 5 ? frame(0xE0, [reason, 0x00]) : frame(0xE0, [])
    }

    static func auth(reason: UInt8, method: String) -> Data {
        frame(0xF0, [reason] + properties([[0x15] + string(method)]))
    }
}

extension BrokerEncoder {
    static func maximumQoS(_ qos: UInt8) -> [UInt8] { [0x24, qos] }
}
