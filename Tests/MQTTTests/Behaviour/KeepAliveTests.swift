import Foundation
import Testing
@testable import MQTT

@Suite(.timeLimit(.minutes(1)))
struct KeepAliveTests {
    @Test(arguments: [Version.v3_1_1, .v5_0])
    func idleClientPingsAndStaysOpen(version: Version) async throws {
        try await withHarness(version) { h in
            var options = OpenOptions()
            options.keepAlive = 1
            options.pingEnabled = true
            let connection = try await h.connect(options)
            #expect(try await connection.next(within: .seconds(3)) == .pingreq)
            connection.send(BrokerEncoder.pingresp)
            #expect(try await connection.next(within: .seconds(3)) == .pingreq)
            #expect(await h.client.status == .opened)
        }
    }

    @Test
    func unansweredPingClosesConnection() async throws {
        try await withHarness(.v5_0) { h in
            var options = OpenOptions()
            options.keepAlive = 1
            options.pingEnabled = true
            options.pingTimeout = .milliseconds(300)
            let connection = try await h.connect(options)
            #expect(try await connection.next(within: .seconds(3)) == .pingreq)
            let closed = try await h.client.events.waitForStatus { $0.isClosed }
            #expect(closed.closeReason == .pingTimeout)
        }
    }
}
