//
//  Transport.swift
//  swift-mqtt
//

import Foundation
import Network
import Synchronization

/// An async byte-stream connection to the broker over TCP, TLS, WebSocket or QUIC.
///
/// Packet framing is left to `PacketFramer`; this type only moves bytes. Only one
/// task may call `receive()` at a time. Calls to `send(_:)` reach the network in the
/// order they are made, because the send is queued before the call suspends.
final class Transport: Sendable {
    private enum State {
        case idle
        case connecting(CheckedContinuation<Void, any Error>)
        case ready
        /// The connection failed or was closed. Later calls throw this error.
        case finished(any Error)
    }

    private let connection: NWConnection
    private let isWebSocket: Bool
    private let queue = DispatchQueue(label: "mqtt.transport")
    private let state = Mutex<State>(.idle)

    init(to endpoint: NWEndpoint, using parameters: NWParameters, isWebSocket: Bool) {
        self.connection = NWConnection(to: endpoint, using: parameters)
        self.isWebSocket = isWebSocket
    }

    convenience init(_ endpoint: Endpoint, config: Config) {
        let (nw, parameters) = endpoint.params(config: config)
        self.init(to: nw, using: parameters, isWebSocket: endpoint.isWebSocket)
    }

    deinit {
        connection.cancel()
    }

    /// Opens the connection, returning once it is ready to carry data.
    ///
    /// Throws the network error if the connection fails or cannot currently be made,
    /// or `CancellationError` if the calling task is cancelled first.
    func connect() async throws {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let started = state.withLock { state in
                    guard case .idle = state else { return false }
                    state = .connecting(continuation)
                    return true
                }
                guard started else {
                    continuation.resume(throwing: MQTTError.alreadyOpened)
                    return
                }
                connection.stateUpdateHandler = { [weak self] in self?.update($0) }
                connection.start(queue: queue)
            }
        } onCancel: {
            finish(CancellationError())
        }
    }

    func send(_ data: Data) async throws {
        if let error = terminalError { throw error }
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
            connection.send(
                content: data,
                contentContext: isWebSocket ? .webSocketBinary : .defaultMessage,
                isComplete: true,
                completion: .contentProcessed { [weak self] error in
                    if let error {
                        continuation.resume(throwing: self?.terminalError ?? error)
                    } else {
                        continuation.resume()
                    }
                }
            )
        }
    }

    /// Returns the next bytes from the broker, which may be empty.
    ///
    /// Throws once the connection has failed, been closed, or reached end of stream.
    func receive() async throws -> Data {
        if let error = terminalError { throw error }
        return try await withCheckedThrowingContinuation { continuation in
            let completion: @Sendable (Data?, NWConnection.ContentContext?, Bool, NWError?) -> Void = {
                [weak self, isWebSocket] content, _, isComplete, error in
                if let error {
                    continuation.resume(throwing: self?.terminalError ?? error)
                } else if let content, !content.isEmpty {
                    continuation.resume(returning: content)
                } else if isComplete, !isWebSocket {
                    // For a stream, isComplete with no data is end of stream.
                    // For WebSocket it only marks the end of one message.
                    let error = MQTTError.decodeError(.streamIsComplete)
                    self?.finish(error)
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume(returning: Data())
                }
            }
            if isWebSocket {
                connection.receiveMessage(completion: completion)
            } else {
                connection.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024, completion: completion)
            }
        }
    }

    /// Closes the connection. Pending and later calls throw.
    func close() {
        finish(MQTTError.unconnected)
    }

    private var terminalError: (any Error)? {
        state.withLock { state in
            if case .finished(let error) = state { return error }
            return nil
        }
    }

    private func update(_ newState: NWConnection.State) {
        switch newState {
        case .ready:
            let connecting = state.withLock { state -> CheckedContinuation<Void, any Error>? in
                guard case .connecting(let continuation) = state else { return nil }
                state = .ready
                return continuation
            }
            connecting?.resume()
        case .waiting(let error), .failed(let error):
            // `.waiting` means the connection can't be made right now (refused,
            // unreachable). Treat it as a failure rather than retrying silently.
            finish(error)
        case .cancelled:
            finish(MQTTError.unconnected)
        default:
            break
        }
    }

    /// Moves to the finished state once, failing a pending `connect()` and
    /// cancelling the connection so pending sends and receives fail too.
    private func finish(_ error: any Error) {
        let connecting = state.withLock { state -> CheckedContinuation<Void, any Error>?? in
            switch state {
            case .finished:
                return nil
            case .connecting(let continuation):
                state = .finished(error)
                return .some(continuation)
            case .idle, .ready:
                state = .finished(error)
                return .some(nil)
            }
        }
        guard let connecting else { return }
        connecting?.resume(throwing: error)
        connection.cancel()
    }
}

extension Endpoint {
    var isWebSocket: Bool {
        switch type {
        case .ws, .wss: true
        case .tcp, .tls, .quic: false
        }
    }
}

private extension NWConnection.ContentContext {
    static let webSocketBinary = NWConnection.ContentContext(
        identifier: "swift-mqtt",
        metadata: [NWProtocolWebSocket.Metadata(opcode: .binary)]
    )
}
