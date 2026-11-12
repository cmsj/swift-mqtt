import Foundation
import Network
import Synchronization

/// A scriptable in-process MQTT broker listening on localhost.
///
/// Tests drive it step by step: wait for a client connection, read the packets the
/// client sends, and reply with hand-built packets (or withhold replies, or drop the
/// connection) to steer the client through specific protocol paths.
///
/// All Network.framework callbacks run on one serial queue; the only state shared
/// with test code is behind a `Mutex` or a `Mailbox`.
final class FakeBroker: Sendable {
    private let queue = DispatchQueue(label: "fake-broker")
    private let listener: NWListener
    private let connections = Mailbox<BrokerConnection>("client connection")
    private let accepted = Mutex<[BrokerConnection]>([])

    private init(listener: NWListener) {
        self.listener = listener
    }

    /// Starts a broker on an ephemeral localhost port.
    static func start() async throws -> FakeBroker {
        let parameters = NWParameters.tcp
        parameters.requiredLocalEndpoint = .hostPort(host: "127.0.0.1", port: .any)
        let broker = FakeBroker(listener: try NWListener(using: parameters))
        try await broker.listen()
        return broker
    }

    var port: UInt16 { listener.port?.rawValue ?? 0 }

    /// Number of connections accepted so far.
    var connectionCount: Int { accepted.withLock { $0.count } }

    private func listen() async throws {
        let ready = Mailbox<Result<Void, NWError>>("listener ready")
        listener.stateUpdateHandler = { state in
            switch state {
            case .ready: ready.push(.success(()))
            case .failed(let error), .waiting(let error): ready.push(.failure(error))
            default: break
            }
        }
        listener.newConnectionHandler = { [weak self] connection in
            self?.accept(connection)
        }
        listener.start(queue: queue)
        try await ready.next().get()
    }

    private func accept(_ connection: NWConnection) {
        let client = BrokerConnection(connection, queue: queue)
        accepted.withLock { $0.append(client) }
        connections.push(client)
        client.start()
    }

    /// Waits for the client's next TCP connection.
    func nextConnection(within timeout: Duration = .prompt) async throws -> BrokerConnection {
        try await connections.next(within: timeout)
    }

    func stop() {
        listener.cancel()
        for connection in accepted.withLock({ $0 }) { connection.drop() }
        connections.finish()
    }
}

/// One client connection accepted by `FakeBroker`.
final class BrokerConnection: Sendable {
    private let connection: NWConnection
    private let queue: DispatchQueue
    private let inbox = Mailbox<ClientPacket>("client packet")
    /// 4 for v3.1.1, 5 for v5. Learned from the client's CONNECT.
    private let level = Mutex<UInt8>(5)

    init(_ connection: NWConnection, queue: DispatchQueue) {
        self.connection = connection
        self.queue = queue
    }

    var protocolLevel: UInt8 { level.withLock { $0 } }

    func start() {
        connection.stateUpdateHandler = { [weak self] state in
            switch state {
            case .failed, .cancelled: self?.inbox.finish()
            default: break
            }
        }
        connection.start(queue: queue)
        readHeader()
    }

    // MARK: Reading (always on `queue`)

    private func receive(_ count: Int, _ handler: @escaping @Sendable ([UInt8]) -> Void) {
        connection.receive(minimumIncompleteLength: count, maximumLength: count) { [weak self] data, _, _, error in
            guard error == nil, let data, data.count == count else {
                self?.inbox.finish()
                return
            }
            handler(Array(data))
        }
    }

    private func readHeader() {
        receive(1) { [weak self] bytes in
            self?.readLength(header: bytes[0], value: 0, shift: 0)
        }
    }

    private func readLength(header: UInt8, value: Int, shift: Int) {
        receive(1) { [weak self] bytes in
            guard let self else { return }
            let value = value | Int(bytes[0] & 0x7F) << shift
            if bytes[0] & 0x80 != 0 {
                readLength(header: header, value: value, shift: shift + 7)
            } else if value == 0 {
                dispatch(header: header, body: [])
            } else {
                receive(value) { [weak self] body in
                    self?.dispatch(header: header, body: body)
                }
            }
        }
    }

    private func dispatch(header: UInt8, body: [UInt8]) {
        do {
            let packet = try BrokerDecoder.decode(header: header, body: body, protocolLevel: protocolLevel)
            if case .connect(let connect) = packet {
                level.withLock { $0 = connect.protocolLevel }
            }
            inbox.push(packet)
            readHeader()
        } catch {
            drop()
        }
    }

    // MARK: Test API

    /// Waits for the next packet the client sends on this connection.
    func next(within timeout: Duration = .prompt) async throws -> ClientPacket {
        try await inbox.next(within: timeout)
    }

    func send(_ data: Data) {
        connection.send(content: data, completion: .idempotent)
    }

    /// Abruptly closes the TCP connection, as if the network or broker failed.
    func drop() {
        connection.cancel()
        inbox.finish()
    }
}

// MARK: Typed expectations

enum UnexpectedPacket: Error, CustomStringConvertible {
    case expected(String, got: ClientPacket)
    var description: String {
        switch self {
        case .expected(let what, let got): "expected \(what), got \(got)"
        }
    }
}

extension BrokerConnection {
    func expectConnect(within timeout: Duration = .prompt) async throws -> ClientPacket.Connect {
        let packet = try await next(within: timeout)
        guard case .connect(let connect) = packet else { throw UnexpectedPacket.expected("CONNECT", got: packet) }
        return connect
    }

    func expectPublish(within timeout: Duration = .prompt) async throws -> ClientPacket.Publish {
        let packet = try await next(within: timeout)
        guard case .publish(let publish) = packet else { throw UnexpectedPacket.expected("PUBLISH", got: packet) }
        return publish
    }

    /// Replies to the client's CONNECT with a CONNACK, returning the CONNECT.
    @discardableResult
    func acceptConnect(sessionPresent: Bool = false, properties: [[UInt8]] = []) async throws -> ClientPacket.Connect {
        let connect = try await expectConnect()
        send(BrokerEncoder.connack(sessionPresent: sessionPresent, protocolLevel: connect.protocolLevel, properties: properties))
        return connect
    }
}
