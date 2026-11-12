import Foundation
import Network
import Testing
@testable import MQTT

@Suite(.timeLimit(.minutes(1)))
struct TransportTests {
    private func tcpTransport(port: UInt16) -> Transport {
        Transport(to: .hostPort(host: "127.0.0.1", port: .init(rawValue: port)!), using: .tcp, isWebSocket: false)
    }

    @Test
    func sendsAndReceivesPackets() async throws {
        let broker = try await FakeBroker.start()
        defer { broker.stop() }
        let transport = tcpTransport(port: broker.port)
        try await transport.connect()

        let connect = ConnectPacket(cleanSession: true, keepAlive: 60, clientId: "t", username: nil, password: nil, properties: [], will: nil)
        try await transport.send(Data(try encode(connect, .v5_0)))
        let connection = try await broker.nextConnection()
        #expect(try await connection.expectConnect().clientId == "t")

        connection.send(BrokerEncoder.connack(sessionPresent: true, protocolLevel: 5))
        connection.send(BrokerEncoder.pingresp)
        var framer = PacketFramer()
        var packets: [any Packet] = []
        while packets.count < 2 {
            framer.append(try await transport.receive())
            while let incoming = try framer.next() {
                packets.append(try incoming.packet(with: .v5_0))
            }
        }
        #expect((packets[0] as? ConnackPacket)?.sessionPresent == true)
        #expect(packets[1] is PingrespPacket)
        transport.close()
    }

    @Test
    func refusedConnectionThrows() async throws {
        // Nothing listens on port 1 (tcpmux), so the kernel refuses the connection.
        let transport = tcpTransport(port: 1)
        await #expect(throws: NWError.self) {
            try await transport.connect()
        }
    }

    @Test
    func brokerClosingConnectionEndsReceive() async throws {
        let broker = try await FakeBroker.start()
        defer { broker.stop() }
        let transport = tcpTransport(port: broker.port)
        try await transport.connect()
        try await transport.send(Data([0xC0, 0x00]))
        let connection = try await broker.nextConnection()
        _ = try await connection.next()
        connection.drop()
        await #expect(throws: (any Error).self) {
            while true { _ = try await transport.receive() }
        }
    }

    @Test
    func closeFailsPendingReceive() async throws {
        let broker = try await FakeBroker.start()
        defer { broker.stop() }
        let transport = tcpTransport(port: broker.port)
        try await transport.connect()
        let receiving = Task { try await transport.receive() }
        _ = try await broker.nextConnection()
        transport.close()
        await #expect(throws: (any Error).self) {
            try await receiving.value
        }
        await #expect(throws: MQTTError.unconnected) {
            try await transport.send(Data([0xC0, 0x00]))
        }
    }

    @Test
    func cancellingConnectThrowsCancellationError() async throws {
        // TEST-NET-1 (RFC 5737) is never routed, so the connection stays in preparing.
        let transport = Transport(to: .hostPort(host: "192.0.2.1", port: 1883), using: .tcp, isWebSocket: false)
        let connecting = Task { try await transport.connect() }
        try await Task.sleep(for: .milliseconds(100))
        connecting.cancel()
        await #expect(throws: CancellationError.self) {
            try await connecting.value
        }
    }
}
