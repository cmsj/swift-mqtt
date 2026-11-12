import Foundation
import Synchronization

enum MailboxError: Error, CustomStringConvertible {
    case timedOut(String)
    case closed(String)

    var description: String {
        switch self {
        case .timedOut(let what): "timed out waiting for \(what)"
        case .closed(let what): "closed while waiting for \(what)"
        }
    }
}

/// A thread-safe FIFO queue with one asynchronous reader.
///
/// `push` is synchronous, so producers on a serial queue (Network.framework callbacks,
/// a single event-stream consumer) deliver elements in the order they happened.
final class Mailbox<Element: Sendable>: Sendable {
    private struct State {
        var items: [Element] = []
        var waiter: (id: UInt64, continuation: CheckedContinuation<Element, any Error>)?
        var nextWaiterID: UInt64 = 0
        var isFinished = false
    }

    private enum Outcome {
        case element(Element)
        case finished
        case waiting(UInt64)
    }

    private let name: String
    private let state = Mutex(State())

    init(_ name: String) { self.name = name }

    func push(_ element: Element) {
        let waiter = state.withLock { state in
            guard let waiter = state.waiter else {
                state.items.append(element)
                return nil as CheckedContinuation<Element, any Error>?
            }
            state.waiter = nil
            return waiter.continuation
        }
        waiter?.resume(returning: element)
    }

    func finish() {
        let waiter = state.withLock { state in
            state.isFinished = true
            defer { state.waiter = nil }
            return state.waiter?.continuation
        }
        waiter?.resume(throwing: MailboxError.closed(name))
    }

    /// Waits for the next element, failing with `MailboxError.timedOut` after `timeout`.
    func next(within timeout: Duration = .prompt) async throws -> Element {
        try await withCheckedThrowingContinuation { continuation in
            let outcome = state.withLock { state -> Outcome in
                if !state.items.isEmpty { return .element(state.items.removeFirst()) }
                if state.isFinished { return .finished }
                precondition(state.waiter == nil, "Mailbox \(name) supports one waiter at a time")
                state.nextWaiterID += 1
                state.waiter = (state.nextWaiterID, continuation)
                return .waiting(state.nextWaiterID)
            }
            switch outcome {
            case .element(let element):
                continuation.resume(returning: element)
            case .finished:
                continuation.resume(throwing: MailboxError.closed(name))
            case .waiting(let id):
                Task {
                    try? await Task.sleep(for: timeout)
                    self.timeOut(id)
                }
            }
        }
    }

    private func timeOut(_ id: UInt64) {
        let waiter = state.withLock { state in
            guard let waiter = state.waiter, waiter.id == id else { return nil as CheckedContinuation<Element, any Error>? }
            state.waiter = nil
            return waiter.continuation
        }
        waiter?.resume(throwing: MailboxError.timedOut(name))
    }
}

extension Duration {
    /// Default wait for anything the client or broker should do promptly.
    static let prompt: Duration = .seconds(5)
}
