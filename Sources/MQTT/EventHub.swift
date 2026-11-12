//
//  EventHub.swift
//  swift-mqtt
//

import Foundation
import Synchronization

/// Fans client events out to any number of `AsyncStream` subscribers, in order.
final class EventHub: Sendable {
    private let subscribers = Mutex<[UUID: AsyncStream<MQTTEvent>.Continuation]>([:])

    func subscribe() -> AsyncStream<MQTTEvent> {
        let (stream, continuation) = AsyncStream.makeStream(of: MQTTEvent.self)
        let id = UUID()
        continuation.onTermination = { [weak self] _ in
            _ = self?.subscribers.withLock { $0.removeValue(forKey: id) }
        }
        subscribers.withLock { $0[id] = continuation }
        return stream
    }

    func emit(_ event: MQTTEvent) {
        for continuation in subscribers.withLock({ Array($0.values) }) {
            continuation.yield(event)
        }
    }

    func finish() {
        for continuation in subscribers.withLock({ Array($0.values) }) {
            continuation.finish()
        }
    }
}
