//
//  PacketFramer.swift
//  swift-mqtt
//

import Foundation

/// Splits an incoming byte stream into MQTT packets.
///
/// Bytes may arrive in any chunking: a chunk can hold part of a packet or several
/// packets. MQTT over WebSocket makes no promise that packets align with WebSocket
/// frames either, so every transport goes through this.
struct PacketFramer: Sendable {
    private var pending = Data()

    mutating func append(_ data: Data) {
        pending.append(data)
    }

    /// Returns the next complete packet, or `nil` if more bytes are needed.
    mutating func next() throws -> IncomingPacket? {
        guard !pending.isEmpty else { return nil }
        var buffer = DataBuffer(data: pending)
        do {
            let packet = try IncomingPacket.read(from: &buffer)
            pending = pending.subdata(in: buffer.nextIndex..<pending.count)
            return packet
        } catch MQTTError.incompletePacket {
            return nil
        }
    }
}
