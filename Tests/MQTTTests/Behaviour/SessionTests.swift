import Foundation
import Testing
@testable import MQTT

/// Disconnects, reconnects and session resumption.
@Suite(.timeLimit(.minutes(1)))
struct SessionTests {
    @Test
    func serverDisconnectFailsPendingRequestsAndCloses() async throws {
        try await withHarness(.v5_0) { h in
            let connection = try await h.connect()
            let publishing = Task { try await h.client.publish("a/b", "x", qos: .atLeastOnce) }
            _ = try await connection.expectPublish()
            connection.send(BrokerEncoder.disconnect(reason: 0x8B, protocolLevel: 5))
            await #expect(throws: MQTTError.serverClose(.serverShuttingDown)) {
                try await publishing.value
            }
            let closed = try await h.client.events.waitForStatus { $0.isClosed }
            #expect(closed.closeReason == .serverClose(.serverShuttingDown))
        }
    }

    @Test(arguments: [Version.v3_1_1, .v5_0])
    func droppedConnectionClosesWithoutRetrier(version: Version) async throws {
        try await withHarness(version) { h in
            let connection = try await h.connect()
            connection.drop()
            let closed = try await h.client.events.waitForStatus { $0.isClosed }
            #expect(closed.closeReason != nil)
            try await Task.sleep(for: .milliseconds(300))
            #expect(h.broker.connectionCount == 1)
        }
    }

    @Test(arguments: [Version.v3_1_1, .v5_0])
    func retrierReconnectsWithoutCleanStart(version: Version) async throws {
        try await withHarness(version) { h in
            var options = OpenOptions()
            options.retry = .init(interval: 0.1, limit: 3)
            let first = try await h.connect(options)
            #expect(try await h.client.events.waitForStatus { $0 == .opened } == .opened)

            first.drop()
            let second = try await h.broker.nextConnection()
            let connect = try await second.acceptConnect()
            #expect(connect.cleanStart == false)
            #expect(try await h.client.events.waitForStatus { $0 == .opened } == .opened)
        }
    }

    @Test(arguments: [Version.v3_1_1, .v5_0])
    func unacknowledgedPublishIsResentWithDupWhenSessionResumes(version: Version) async throws {
        try await withHarness(version) { h in
            var options = OpenOptions()
            options.retry = .init(interval: 0.1, limit: 3)
            let first = try await h.connect(options)
            // The call itself fails when the connection drops; this checks the resend.
            let publishing = Task { try await h.client.publish("a/b", "x", qos: .atLeastOnce) }
            let original = try await first.expectPublish()
            first.drop()

            let second = try await h.broker.nextConnection()
            try await second.acceptConnect(sessionPresent: true)
            let resent = try await second.expectPublish()
            #expect(resent.id == original.id)
            #expect(resent.dup)
            #expect(resent.topic == original.topic)
            second.send(BrokerEncoder.ack(0x40, id: resent.id, protocolLevel: h.protocolLevel))
            _ = try? await publishing.value
        }
    }

    @Test
    func noResendWhenSessionIsNotPresent() async throws {
        try await withHarness(.v5_0) { h in
            var options = OpenOptions()
            options.retry = .init(interval: 0.1, limit: 3)
            let first = try await h.connect(options)
            let publishing = Task { try await h.client.publish("a/b", "x", qos: .atLeastOnce) }
            _ = try await first.expectPublish()
            first.drop()

            let second = try await h.broker.nextConnection()
            try await second.acceptConnect(sessionPresent: false)
            await #expect(throws: MailboxError.self) {
                try await second.next(within: .milliseconds(300))
            }
            _ = try? await publishing.value
        }
    }
}
