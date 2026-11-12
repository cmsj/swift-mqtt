//
//  Client.swift
//  swift-mqtt
//
//  Created by supertext on 2024/12/23.
//

import Foundation
import Network

/// An MQTT 3.1.1 or 5.0 client.
///
/// All state lives in the actor. Inbound packets are handled synchronously, one at a time
/// and in arrival order, so message delivery order matches the wire.
public actor MQTTClient {
    public nonisolated let endpoint: Endpoint
    public nonisolated let version: Version
    /// Settings used by the next `open`.
    public private(set) var config: Config
    /// The current connection status.
    public private(set) var status: Status = .closed()
    /// The identity passed to `open`, with any client id the broker assigned.
    public private(set) var identity: Identity?

    private let hub = EventHub()

    // Session state. Survives reconnects; cleared when the broker has no session.
    private var connectPacket: ConnectPacket?
    private var authflow: Authflow?
    private var lastPacketId: UInt16 = 0
    /// Outbound QoS 1/2 flows awaiting acknowledgement: the PUBLISH, or the PUBREL once PUBREC arrived.
    private var inflight: [UInt16: any Packet] = [:]
    /// Inbound QoS 2 messages awaiting PUBREL, delivered once it arrives.
    private var awaitingRelease: [UInt16: Message] = [:]

    // Connection state.
    private var link: Link?
    private var linkCount: UInt64 = 0
    private var limits = ServerLimits()
    private var keepAlive: Duration = .zero
    private var lastSend: ContinuousClock.Instant = .now
    private var pending: [RequestKey: PendingRequest] = [:]
    private var requestCount: UInt64 = 0
    private var retryAttempt: UInt = 0
    private var retryTask: Task<Void, Never>?
    private var monitorTask: Task<Void, Never>?

    public init(_ endpoint: Endpoint, version: Version, config: Config = Config()) {
        self.endpoint = endpoint
        self.version = version
        self.config = config
    }

    deinit {
        retryTask?.cancel()
        monitorTask?.cancel()
        link?.close()
        hub.finish()
    }

    /// Replaces the settings. They take effect on the next `open`.
    public func configure(_ config: Config) {
        self.config = config
    }

    /// A stream of status changes, received messages and connection errors.
    ///
    /// Each call returns an independent stream; all of them see every event, in order.
    public nonisolated func events() -> AsyncStream<MQTTEvent> {
        hub.subscribe()
    }
}

// MARK: - Public API

extension MQTTClient {
    /// Connects to the broker.
    ///
    /// If `cleanStart` is false the broker resumes the session for this client id, if it has one.
    /// On MQTT v5 the session is then kept for as long as the broker allows.
    ///
    /// - Parameters:
    ///   - properties: MQTT v5 `CONNECT` properties. Must be empty for v3.1.1.
    ///   - authflow: MQTT v5 enhanced authentication. Called for each `AUTH` challenge.
    @discardableResult
    public func open(
        _ identity: Identity,
        will: Will? = nil,
        cleanStart: Bool = true,
        properties: Properties = [],
        authflow: Authflow? = nil
    ) async throws -> Connack {
        guard status.isClosed else { throw MQTTError.alreadyOpened }
        try requireV5(unless: properties.isEmpty && (will?.properties.isEmpty ?? true) && authflow == nil)
        var properties = properties
        if version == .v5_0, !cleanStart {
            properties.append(.sessionExpiryInterval(0xFFFF_FFFF))
        }
        let message = will.map {
            Message(qos: $0.qos, dup: false, topic: $0.topic, retain: $0.retain, payload: $0.payload, properties: $0.properties)
        }
        connectPacket = ConnectPacket(
            cleanSession: cleanStart,
            keepAlive: config.keepAlive,
            clientId: identity.clientId,
            username: identity.username,
            password: identity.password,
            properties: properties,
            will: message
        )
        self.identity = identity
        self.authflow = authflow
        retryAttempt = 0
        startMonitor()
        setStatus(.opening)
        do {
            return try await connect().ack()
        } catch {
            connectionFailed(CloseReason(error: error), error: error, link: linkCount)
            throw error
        }
    }

    /// Disconnects from the broker, sending `DISCONNECT` if connected.
    public func close(_ code: ResultCode.Disconnect = .normal, properties: Properties = []) async throws {
        try requireV5(unless: properties.isEmpty)
        stopMonitor()
        switch status {
        case .closing, .closed:
            throw MQTTError.alreadyClosed
        case .opening:
            finish(.clientClose(code), error: MQTTError.clientClose(code))
        case .opened:
            setStatus(.closing)
            try? await flush(DisconnectPacket(code: code, properties: properties))
            finish(.clientClose(code), error: MQTTError.clientClose(code))
        }
    }

    /// Publishes a message.
    ///
    /// - Returns: `nil` for QoS 0, which completes once the message is sent. For QoS 1 and 2,
    ///   the broker's final acknowledgement.
    /// - Throws: `MQTTError.publishFailed` if the broker rejects the message, or
    ///   `MQTTError.timeout` if it doesn't answer within `Config.requestTimeout`.
    @discardableResult
    public func publish(
        to topic: String,
        payload: Data,
        qos: MQTTQoS = .atLeastOnce,
        retain: Bool = false,
        properties: Properties = []
    ) async throws -> Puback? {
        guard status == .opened else { throw MQTTError.unconnected }
        try requireV5(unless: properties.isEmpty)
        try validatePublish(topic: topic, qos: qos, retain: retain, properties: properties)
        let message = Message(qos: qos, dup: false, topic: topic, retain: retain, payload: payload, properties: properties)
        if qos == .atMostOnce {
            try await flush(PublishPacket(id: 0, message: message))
            return nil
        }
        let packet = PublishPacket(id: nextPacketId(), message: message)
        inflight[packet.id] = packet
        let ack = try await request(packet, key: .packet(packet.id), timeout: config.requestTimeout)
        guard let ack = ack as? PubackPacket else { throw MQTTError.unexpectMessage }
        switch (qos, ack.type) {
        case (.atLeastOnce, .PUBACK):
            inflight[packet.id] = nil
            return try accepted(ack)
        case (.exactlyOnce, .PUBREC):
            if ack.code.rawValue > 0x7F {
                inflight[packet.id] = nil
                throw MQTTError.publishFailed(ack.code)
            }
            return try await release(packet.id)
        default:
            throw MQTTError.unexpectMessage
        }
    }

    @discardableResult
    public func publish(
        to topic: String,
        payload: String,
        qos: MQTTQoS = .atLeastOnce,
        retain: Bool = false,
        properties: Properties = []
    ) async throws -> Puback? {
        try await publish(to: topic, payload: Data(payload.utf8), qos: qos, retain: retain, properties: properties)
    }

    /// Subscribes to topic filters.
    /// - Parameter properties: MQTT v5 `SUBSCRIBE` properties. Must be empty for v3.1.1.
    @discardableResult
    public func subscribe(to subscriptions: [Subscribe], properties: Properties = []) async throws -> Suback {
        guard status == .opened else { throw MQTTError.unconnected }
        try requireV5(unless: properties.isEmpty)
        guard !subscriptions.isEmpty else { throw MQTTError.packetError(.atLeastOneTopicRequired) }
        let packet = SubscribePacket(id: nextPacketId(), properties: properties, subscriptions: subscriptions)
        let reply = try await request(packet, key: .packet(packet.id), timeout: config.requestTimeout)
        guard let suback = reply as? SubackPacket, suback.type == .SUBACK else { throw MQTTError.unexpectMessage }
        return suback.suback()
    }

    @discardableResult
    public func subscribe(to topic: String, qos: MQTTQoS = .atLeastOnce, properties: Properties = []) async throws -> Suback {
        try await subscribe(to: [Subscribe(topic, qos: qos)], properties: properties)
    }

    /// Unsubscribes from topic filters.
    /// - Returns: The broker's reason codes. MQTT v3.1.1 has none, so they are empty.
    @discardableResult
    public func unsubscribe(from topics: [String], properties: Properties = []) async throws -> Unsuback {
        guard status == .opened else { throw MQTTError.unconnected }
        try requireV5(unless: properties.isEmpty)
        guard !topics.isEmpty else { throw MQTTError.packetError(.atLeastOneTopicRequired) }
        let packet = UnsubscribePacket(id: nextPacketId(), subscriptions: topics, properties: properties)
        let reply = try await request(packet, key: .packet(packet.id), timeout: config.requestTimeout)
        guard let unsuback = reply as? SubackPacket, unsuback.type == .UNSUBACK else { throw MQTTError.unexpectMessage }
        return unsuback.unsuback()
    }

    @discardableResult
    public func unsubscribe(from topic: String, properties: Properties = []) async throws -> Unsuback {
        try await unsubscribe(from: [topic], properties: properties)
    }

    /// Re-authenticates with the broker (MQTT v5 only).
    ///
    /// - Parameters:
    ///   - properties: Must include `authenticationMethod`.
    ///   - authflow: Answers each `AUTH` challenge from the broker.
    /// - Returns: The broker's final `AUTH`.
    @discardableResult
    public func auth(_ properties: Properties, authflow: Authflow? = nil) async throws -> Auth {
        try requireV5(unless: false)
        guard status == .opened else { throw MQTTError.unconnected }
        var reply = try await request(AuthPacket(code: .reAuthenticate, properties: properties), key: .auth, timeout: config.requestTimeout)
        while true {
            guard let auth = reply as? AuthPacket else { throw MQTTError.unexpectMessage }
            switch auth.code {
            case .success:
                return auth.ack()
            case .continueAuthentication:
                guard let authflow else { throw MQTTError.authflowRequired }
                let response = try await authflow(auth.ack())
                reply = try await request(response.packet(), key: .auth, timeout: config.requestTimeout)
            default:
                throw MQTTError.unexpectMessage
            }
        }
    }
}

// MARK: - Connection lifecycle

extension MQTTClient {
    /// One network connection and the tasks serving it.
    private struct Link: Sendable {
        let id: UInt64
        let transport: Transport
        let outbound: AsyncStream<Outbound>.Continuation
        var tasks: [Task<Void, Never>] = []

        func close() {
            outbound.finish()
            transport.close()
            for task in tasks { task.cancel() }
        }
    }

    private struct Outbound: Sendable {
        let data: Data
        let sent: CheckedContinuation<Void, any Error>?
    }

    /// Limits announced by the broker in `CONNACK`.
    private struct ServerLimits {
        var maxQoS: MQTTQoS = .exactlyOnce
        var retainAvailable = true
        var maxTopicAlias: UInt16 = 65535
    }

    /// Opens a new connection and performs the `CONNECT` handshake, including any v5 `AUTH` exchange.
    private func connect() async throws -> ConnackPacket {
        guard var packet = connectPacket else { throw MQTTError.connectFailed() }
        link?.close()
        linkCount += 1
        let id = linkCount
        let transport = Transport(endpoint, config: config)
        let (stream, outbound) = AsyncStream.makeStream(of: Outbound.self)
        link = Link(id: id, transport: transport, outbound: outbound)

        try await withThrowingTimeout(config.connectTimeout) {
            try await transport.connect()
        }
        guard link?.id == id else { throw MQTTError.clientClose(.normal) }
        link?.tasks = [
            Task { [transport] in
                for await item in stream {
                    do {
                        try await transport.send(item.data)
                        item.sent?.resume()
                    } catch {
                        item.sent?.resume(throwing: error)
                    }
                }
            },
            Task { [weak self, transport, version] in
                var framer = PacketFramer()
                do {
                    while true {
                        framer.append(try await transport.receive())
                        while let incoming = try framer.next() {
                            let packet = try incoming.packet(with: version)
                            guard let self else { return }
                            await self.received(packet, link: id)
                        }
                    }
                } catch {
                    await self?.connectionFailed(CloseReason(error: error), error: error, link: id)
                }
            },
        ]

        var reply = try await request(packet, key: .connect, timeout: config.connectTimeout)
        while let auth = reply as? AuthPacket, auth.code == .continueAuthentication {
            guard let authflow else { throw MQTTError.authflowRequired }
            let response = try await authflow(auth.ack())
            reply = try await request(response.packet(), key: .connect, timeout: config.connectTimeout)
        }
        guard let connack = reply as? ConnackPacket else { throw MQTTError.unexpectMessage }
        try accept(connack)

        // Later reconnects resume this session.
        if packet.cleanSession {
            packet = packet.copyNotClean()
            connectPacket = packet
        }
        retryAttempt = 0
        setStatus(.opened)
        startKeepAlive(link: id)
        if connack.sessionPresent {
            resendInflight()
        } else {
            inflight.removeAll()
            awaitingRelease.removeAll()
        }
        return connack
    }

    private func accept(_ connack: ConnackPacket) throws {
        switch version {
        case .v3_1_1:
            if connack.returnCode != 0 {
                throw MQTTError.connectFailed(.connectv3(ResultCode.ConnectV3(rawValue: connack.returnCode) ?? .unrecognisedReason))
            }
        case .v5_0:
            if connack.returnCode > 0x7F {
                throw MQTTError.connectFailed(.connect(ResultCode.Connect(rawValue: connack.returnCode) ?? .unrecognisedReason))
            }
        }
        limits = ServerLimits()
        keepAlive = .seconds(Int(config.keepAlive))
        for property in connack.properties {
            switch property {
            case .serverKeepAlive(let seconds): keepAlive = .seconds(Int(seconds))
            case .assignedClientIdentifier(let clientId): identity?.clientId = clientId
            case .maximumQoS(let qos): limits.maxQoS = qos
            case .retainAvailable(let available): limits.retainAvailable = available != 0
            case .topicAliasMaximum(let max): limits.maxTopicAlias = max
            default: break
            }
        }
    }

    private func resendInflight() {
        for (_, packet) in inflight.sorted(by: { $0.key < $1.key }) {
            if let publish = packet as? PublishPacket {
                let resend = PublishPacket(id: publish.id, message: publish.message.duplicate())
                inflight[publish.id] = resend
                write(resend)
            } else {
                write(packet)
            }
        }
    }

    /// Handles the loss of connection `link`: closes it, fails pending requests, and either
    /// schedules a reconnect or closes the client. Ignores reports from superseded links.
    private func connectionFailed(_ reason: CloseReason, error: any Error, link id: UInt64) {
        guard id == linkCount, status == .opened || status == .opening else { return }
        hub.emit(.error(error))
        if let delay = retryDelay(for: reason) {
            tearDown(error: error)
            setStatus(.opening)
            retryTask = Task { [weak self] in
                try? await Task.sleep(for: delay)
                guard !Task.isCancelled else { return }
                await self?.reconnect()
            }
        } else {
            finish(reason, error: error)
        }
    }

    private func reconnect() async {
        guard status == .opening else { return }
        do {
            _ = try await connect()
        } catch {
            connectionFailed(CloseReason(error: error), error: error, link: linkCount)
        }
    }

    private func retryDelay(for reason: CloseReason) -> Duration? {
        switch reason {
        case .clientClose, .unsatisfied:
            return nil
        case .networkError(.posix(.ENETUNREACH)), .networkError(.posix(.ENETDOWN)):
            return nil
        default:
            break
        }
        guard let retrier = config.retrier, connectPacket != nil else { return nil }
        retryAttempt += 1
        return retrier.delay(attempt: retryAttempt, reason: reason)
    }

    /// Closes the connection and moves to `.closed(reason)`.
    private func finish(_ reason: CloseReason, error: any Error) {
        tearDown(error: error)
        setStatus(.closed(reason))
    }

    private func tearDown(error: any Error) {
        retryTask?.cancel()
        retryTask = nil
        link?.close()
        link = nil
        linkCount += 1
        let failed = pending
        pending.removeAll()
        for request in failed.values {
            request.timer?.cancel()
            request.continuation.resume(throwing: error)
        }
    }

    private func setStatus(_ new: Status) {
        guard new != status else { return }
        let old = status
        status = new
        logger.debug("STATUS: \(old) --> \(new)")
        hub.emit(.status(new, previous: old))
    }
}

// MARK: - Keep alive and network monitoring

extension MQTTClient {
    private func startKeepAlive(link id: UInt64) {
        guard config.pingEnabled, keepAlive > .zero else { return }
        link?.tasks.append(Task { [weak self] in
            while let deadline = await self?.nextPing(link: id) {
                if deadline > .now {
                    try? await Task.sleep(until: deadline)
                    if Task.isCancelled { return }
                    continue
                }
                await self?.ping(link: id)
            }
        })
    }

    /// When the next ping is due, or `nil` once the link has gone.
    private func nextPing(link id: UInt64) -> ContinuousClock.Instant? {
        guard id == linkCount, status == .opened else { return nil }
        return lastSend + keepAlive
    }

    private func ping(link id: UInt64) async {
        do {
            _ = try await request(PingreqPacket(), key: .ping, timeout: config.pingTimeout)
        } catch MQTTError.timeout {
            connectionFailed(.pingTimeout, error: MQTTError.timeout, link: id)
        } catch {
            // The connection failed or closed, which is handled elsewhere.
        }
    }

    private func startMonitor() {
        guard config.monitorsNetwork, monitorTask == nil else { return }
        monitorTask = Task { [weak self] in
            var previous: NWPath.Status?
            for await path in NWPathMonitor() {
                guard path.status != previous else { continue }
                previous = path.status
                await self?.networkChanged(path.status)
            }
        }
    }

    private func stopMonitor() {
        monitorTask?.cancel()
        monitorTask = nil
    }

    private func networkChanged(_ path: NWPath.Status) async {
        switch (path, status) {
        case (.unsatisfied, .opened), (.unsatisfied, .opening):
            finish(.unsatisfied, error: MQTTError.unconnected)
        case (.satisfied, .closed(.unsatisfied)), (.satisfied, .closed(.networkError)):
            setStatus(.opening)
            await reconnect()
        default:
            break
        }
    }
}

// MARK: - Sending and requests

extension MQTTClient {
    private enum RequestKey: Hashable {
        /// `CONNECT` and `AUTH` during connection: answered by `CONNACK` or `AUTH`.
        case connect
        /// Re-authentication: answered by `AUTH`.
        case auth
        case ping
        /// Requests answered by an acknowledgement carrying the same packet id.
        case packet(UInt16)
    }

    private struct PendingRequest {
        let id: UInt64
        let continuation: CheckedContinuation<any Packet, any Error>
        let timer: Task<Void, Never>?
    }

    /// Sends `packet` and waits for the broker's reply.
    private func request(_ packet: any Packet, key: RequestKey, timeout: Duration) async throws -> any Packet {
        let data = try encode(packet)
        requestCount += 1
        let id = requestCount
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                guard let link else {
                    continuation.resume(throwing: MQTTError.unconnected)
                    return
                }
                let timer = Task { [weak self] in
                    try? await Task.sleep(for: timeout)
                    guard !Task.isCancelled else { return }
                    await self?.complete(key, request: id, with: .failure(MQTTError.timeout))
                }
                pending[key].map { complete(key, request: $0.id, with: .failure(MQTTError.unexpectMessage)) }
                pending[key] = PendingRequest(id: id, continuation: continuation, timer: timer)
                send(data, on: link, sent: nil)
            }
        } onCancel: {
            Task { await self.complete(key, request: id, with: .failure(CancellationError())) }
        }
    }

    /// Resumes the pending request for `key`, if it is still request `id` (or any request when `id` is nil).
    private func complete(_ key: RequestKey, request id: UInt64? = nil, with result: Result<any Packet, any Error>) {
        guard let request = pending[key], id == nil || request.id == id else { return }
        pending[key] = nil
        request.timer?.cancel()
        request.continuation.resume(with: result)
    }

    /// Sends a packet that has no reply, waiting until it has been handed to the network.
    private func flush(_ packet: any Packet) async throws {
        let data = try encode(packet)
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
            guard let link else {
                continuation.resume(throwing: MQTTError.unconnected)
                return
            }
            send(data, on: link, sent: continuation)
        }
    }

    /// Queues a packet that has no reply, without waiting.
    private func write(_ packet: any Packet) {
        guard let link else { return }
        do {
            send(try encode(packet), on: link, sent: nil)
        } catch {
            logger.error("Failed to encode \(packet.description): \(error)")
        }
    }

    private func send(_ data: Data, on link: Link, sent: CheckedContinuation<Void, any Error>?) {
        lastSend = .now
        if case .terminated = link.outbound.yield(Outbound(data: data, sent: sent)) {
            sent?.resume(throwing: MQTTError.unconnected)
        }
    }

    private func encode(_ packet: any Packet) throws -> Data {
        logger.debug("SEND: \(packet.description)")
        var buffer = DataBuffer()
        try packet.write(version: version, to: &buffer)
        return buffer.data
    }

    private func nextPacketId() -> UInt16 {
        repeat {
            lastPacketId = lastPacketId == .max ? 1 : lastPacketId + 1
        } while inflight[lastPacketId] != nil || pending[.packet(lastPacketId)] != nil
        return lastPacketId
    }

    /// Sends PUBREL for a QoS 2 publish and waits for PUBCOMP.
    private func release(_ id: UInt16) async throws -> Puback {
        let pubrel = PubackPacket(id: id, type: .PUBREL)
        inflight[id] = pubrel
        let reply = try await request(pubrel, key: .packet(id), timeout: config.requestTimeout)
        guard let pubcomp = reply as? PubackPacket, pubcomp.type == .PUBCOMP else { throw MQTTError.unexpectMessage }
        inflight[id] = nil
        return try accepted(pubcomp)
    }

    private func accepted(_ ack: PubackPacket) throws -> Puback {
        if ack.code.rawValue > 0x7F {
            throw MQTTError.publishFailed(ack.code)
        }
        return ack.ack()
    }

    private func validatePublish(topic: String, qos: MQTTQoS, retain: Bool, properties: Properties) throws {
        guard limits.maxQoS.rawValue >= qos.rawValue else { throw MQTTError.packetError(.qosInvalid) }
        guard !retain || limits.retainAvailable else { throw MQTTError.packetError(.retainUnavailable) }
        for property in properties {
            if case .topicAlias(let alias) = property, alias == 0 || alias > limits.maxTopicAlias {
                throw MQTTError.packetError(.topicAliasOutOfRange)
            }
            if case .subscriptionIdentifier = property {
                throw MQTTError.packetError(.publishIncludesSubscription)
            }
        }
        guard !topic.contains(where: { $0 == "#" || $0 == "+" }) else { throw MQTTError.packetError(.invalidTopicName) }
    }

    private func requireV5(unless allowed: Bool) throws {
        guard allowed || version == .v5_0 else { throw MQTTError.packetError(.badParameter) }
    }
}

// MARK: - Receiving

extension MQTTClient {
    private func received(_ packet: any Packet, link id: UInt64) {
        guard id == linkCount else { return }
        logger.debug("RECV: \(packet.description)")
        switch packet {
        case is PingrespPacket:
            complete(.ping, with: .success(packet))
        case is PingreqPacket:
            write(PingrespPacket())
        case let disconnect as DisconnectPacket:
            connectionFailed(.serverClose(disconnect.code), error: MQTTError.serverClose(disconnect.code), link: id)
        case is ConnackPacket:
            complete(.connect, with: .success(packet))
        case is AuthPacket:
            complete(pending[.connect] != nil ? .connect : .auth, with: .success(packet))
        case let publish as PublishPacket:
            receivedPublish(publish)
        case let ack as PubackPacket:
            receivedAck(ack)
        case let suback as SubackPacket:
            complete(.packet(suback.id), with: .success(suback))
        default:
            logger.error("Unexpected packet from broker: \(packet.description)")
        }
    }

    private func receivedPublish(_ publish: PublishPacket) {
        switch publish.message.qos {
        case .atMostOnce:
            hub.emit(.message(publish.message))
        case .atLeastOnce:
            write(publish.puback())
            hub.emit(.message(publish.message))
        case .exactlyOnce:
            // Delivered when PUBREL arrives. A redelivered PUBLISH keeps the first copy.
            if awaitingRelease[publish.id] == nil {
                awaitingRelease[publish.id] = publish.message
            }
            write(publish.pubrec())
        }
    }

    private func receivedAck(_ ack: PubackPacket) {
        if ack.type == .PUBREL {
            if let message = awaitingRelease.removeValue(forKey: ack.id) {
                hub.emit(.message(message))
                write(ack.pubcomp())
            } else {
                write(ack.pubcomp(code: version == .v5_0 ? .packetIdentifierNotFound : .success))
            }
            return
        }
        if pending[.packet(ack.id)] != nil {
            complete(.packet(ack.id), with: .success(ack))
            return
        }
        // No caller is waiting: a flow resent after reconnecting, or one whose caller timed out.
        switch ack.type {
        case .PUBACK, .PUBCOMP:
            inflight[ack.id] = nil
        case .PUBREC where inflight[ack.id] is PublishPacket:
            let pubrel = PubackPacket(id: ack.id, type: .PUBREL)
            inflight[ack.id] = pubrel
            write(pubrel)
        case .PUBREC where version == .v5_0:
            write(ack.pubrel(code: .packetIdentifierNotFound))
        default:
            break
        }
    }
}

/// Runs `operation`, cancelling it and throwing `MQTTError.timeout` if it takes longer than `timeout`.
private func withThrowingTimeout<T: Sendable>(
    _ timeout: Duration,
    _ operation: @escaping @Sendable () async throws -> T
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask { try await operation() }
        group.addTask {
            try await Task.sleep(for: timeout)
            throw MQTTError.timeout
        }
        defer { group.cancelAll() }
        return try await group.next()!
    }
}
