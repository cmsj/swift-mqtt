//
//  Retrier.swift
//  swift-mqtt
//
//  Created by supertext on 2024/12/10.
//

import Foundation

/// Automatic reconnection settings.
///
/// Some close reasons are never retried (a client close, the network being unreachable
/// or unavailable). For the rest, `filter` decides.
public struct Retrier: Sendable {
    /// Return `true` to skip reconnecting for this close reason.
    public typealias Filter = @Sendable (CloseReason) -> Bool

    /// Backoff between reconnection attempts.
    public enum Policy: Sendable {
        /// The delay grows linearly: `scale * attempt` seconds
        case linear(scale: Double = 1)
        /// A fixed delay, in seconds
        case equals(interval: TimeInterval = 3)
        /// A random delay in `min...max` seconds
        case random(min: TimeInterval = 2, max: TimeInterval = 5)
        /// The delay grows exponentially: `scale * base^attempt` seconds, capped at `max`
        case exponential(base: Int = 2, scale: Double = 1, max: TimeInterval = 120)
    }

    public var policy: Policy
    /// Maximum number of consecutive attempts. The count resets once the client opens.
    public var limits: UInt
    /// Return `true` to skip reconnecting for a close reason.
    public var filter: Filter?

    public init(_ policy: Policy = .exponential(), limits: UInt = 10, filter: Filter? = nil) {
        self.policy = policy
        self.limits = limits
        self.filter = filter
    }

    /// The delay before reconnection attempt number `attempt` (starting at 1), or `nil` to stop.
    func delay(attempt: UInt, reason: CloseReason) -> Duration? {
        if filter?(reason) == true || attempt > limits {
            return nil
        }
        let seconds: TimeInterval = switch policy {
        case .linear(let scale): scale * Double(attempt)
        case .equals(let interval): interval
        case .random(let min, let max): .random(in: min...max)
        case .exponential(let base, let scale, let max): Swift.min(pow(Double(base), Double(attempt)) * scale, max)
        }
        return .seconds(seconds)
    }
}
