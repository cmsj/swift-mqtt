import Foundation
import Testing
@testable import MQTT

@Suite(.timeLimit(.minutes(1)))
struct ConnectTests {
    @Test(arguments: [Version.v3_1_1, .v5_0])
    func connectSendsIdentityAndOpens(version: Version) async throws {
        try await withHarness(version) { h in
            var options = OpenOptions()
            options.clientId = "client-1"
            options.username = "user"
            options.password = "secret"
            options.keepAlive = 30
            async let opened = h.client.open(options)
            let connection = try await h.broker.nextConnection()
            let connect = try await connection.acceptConnect()
            let result = try await opened

            #expect(connect.protocolLevel == h.protocolLevel)
            #expect(connect.clientId == "client-1")
            #expect(connect.username == "user")
            #expect(connect.password == "secret")
            #expect(connect.keepAlive == 30)
            #expect(connect.cleanStart)
            #expect(result == OpenResult(sessionPresent: false, reasonCode: 0))
            #expect(try await h.client.events.nextStatus() == .opening)
            #expect(try await h.client.events.nextStatus() == .opened)
            #expect(await h.client.status == .opened)
        }
    }

    @Test
    func resumingSessionOnV5RequestsMaximumSessionExpiry() async throws {
        try await withHarness(.v5_0) { h in
            var options = OpenOptions()
            options.cleanStart = false
            async let opened = h.client.open(options)
            let connect = try await h.broker.nextConnection().acceptConnect()
            _ = try await opened
            #expect(connect.cleanStart == false)
            #expect(connect.sessionExpiry == 0xFFFF_FFFF)
        }
    }

    @Test
    func resumingSessionOnV3SendsNoSessionExpiry() async throws {
        try await withHarness(.v3_1_1) { h in
            var options = OpenOptions()
            options.cleanStart = false
            async let opened = h.client.open(options)
            let connect = try await h.broker.nextConnection().acceptConnect()
            _ = try await opened
            #expect(connect.cleanStart == false)
            #expect(connect.sessionExpiry == nil)
        }
    }

    @Test(arguments: [Version.v3_1_1, .v5_0])
    func reportsSessionPresent(version: Version) async throws {
        try await withHarness(version) { h in
            async let opened = h.client.open(.init())
            try await h.broker.nextConnection().acceptConnect(sessionPresent: true)
            #expect(try await opened.sessionPresent)
        }
    }

    @Test(arguments: [
        (Version.v3_1_1, UInt8(5), MQTTError.connectFailed(.connectv3(.notAuthorized))),
        (Version.v5_0, UInt8(0x87), MQTTError.connectFailed(.connect(.notAuthorized))),
    ])
    func refusedConnectionThrowsAndCloses(version: Version, reason: UInt8, error: MQTTError) async throws {
        try await withHarness(version) { h in
            let opening = Task { try await h.client.open(.init()) }
            let connection = try await h.broker.nextConnection()
            _ = try await connection.expectConnect()
            connection.send(BrokerEncoder.connack(reason: reason, protocolLevel: h.protocolLevel))
            await #expect(throws: error) { try await opening.value }
            #expect(try await h.client.events.waitForStatus { $0.isClosed }.isClosed)
        }
    }

    @Test
    func openingTwiceFails() async throws {
        try await withHarness(.v5_0) { h in
            _ = try await h.connect()
            await #expect(throws: MQTTError.alreadyOpened) {
                try await h.client.open(.init())
            }
        }
    }

    @Test(arguments: [Version.v3_1_1, .v5_0])
    func operationsBeforeOpenFail(version: Version) async throws {
        try await withHarness(version) { h in
            await #expect(throws: MQTTError.unconnected) {
                try await h.client.publish("t", "x", qos: .atLeastOnce)
            }
            await #expect(throws: MQTTError.unconnected) {
                try await h.client.subscribe(["t"], qos: .atLeastOnce)
            }
            await #expect(throws: MQTTError.unconnected) {
                try await h.client.unsubscribe(["t"])
            }
        }
    }

    @Test(arguments: [Version.v3_1_1, .v5_0])
    func closeSendsDisconnect(version: Version) async throws {
        try await withHarness(version) { h in
            let connection = try await h.connect()
            try await h.client.close()
            #expect(try await connection.next() == .disconnect(reason: 0))
            #expect(try await h.client.events.waitForStatus { $0.isClosed }.isClosed)
            await #expect(throws: MQTTError.alreadyClosed) {
                try await h.client.close()
            }
        }
    }
}
