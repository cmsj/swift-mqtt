import Foundation
import Testing
@testable import MQTT

/// MQTT 5 enhanced authentication (§4.12).
@Suite(.timeLimit(.minutes(1)))
struct AuthTests {
    @Test
    func challengeDuringConnectIsAnsweredByAuthflow() async throws {
        try await withHarness(.v5_0) { h in
            var options = OpenOptions()
            options.authMethod = "test"
            options.authflow = { challenge in
                #expect(challenge.code == .continueAuthentication)
                return Auth(code: .continueAuthentication, properties: [.authenticationMethod("test")])
            }
            let opening = Task { try await h.client.open(options) }
            let connection = try await h.broker.nextConnection()
            let connect = try await connection.expectConnect()
            #expect(connect.authenticationMethod == "test")

            connection.send(BrokerEncoder.auth(reason: 0x18, method: "test"))
            #expect(try await connection.next() == .auth(reason: 0x18, method: "test"))
            connection.send(BrokerEncoder.connack(protocolLevel: 5))
            #expect(try await opening.value.reasonCode == 0)
            #expect(await h.client.status == .opened)
        }
    }
}
