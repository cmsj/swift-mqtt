//
//  MQTT.swift
//  swift-mqtt
//
//  Created by supertext on 2024/12/10.
//

import Foundation
import os

let logger = os.Logger(subsystem: "swift-mqtt", category: "MQTT")

public enum Version: Sendable {
    case v5_0
    case v3_1_1
    var string: String {
        switch self {
        case .v5_0: return "5.0"
        case .v3_1_1: return "3.1.1"
        }
    }
    var uint8: UInt8 {
        switch self {
        case .v3_1_1: return 4
        case .v5_0: return 5
        }
    }
}

/// Client settings. Changes made with `MQTTClient.configure(_:)` take effect on the next `open`.
public struct Config: Sendable {
    /// MQTT keep alive period in seconds, sent in `CONNECT`. A server may override it in `CONNACK`.
    public var keepAlive: UInt16 = 60
    /// Send `PINGREQ` when the connection has been idle for `keepAlive` seconds.
    public var pingEnabled = true
    /// How long to wait for `PINGRESP` before treating the connection as lost.
    public var pingTimeout: Duration = .seconds(5)
    /// How long to wait for the transport to connect and the broker to answer `CONNECT`.
    /// - Important: QUIC's handshake timeout is fixed by the system at 30 seconds.
    public var connectTimeout: Duration = .seconds(30)
    /// How long to wait for the broker to acknowledge a publish, subscribe, unsubscribe or auth request.
    ///
    /// A QoS 1 or 2 publish that times out is not resent on the same connection (MQTT 5 §4.4).
    /// It stays in the session and is resent if the client reconnects to a resumed session.
    public var requestTimeout: Duration = .seconds(5)
    /// Reconnect automatically after an unexpected disconnect. `nil` disables reconnection.
    public var retrier: Retrier?
    /// Close the connection while the network is unavailable, and reconnect when it returns.
    public var monitorsNetwork = false

    public init() {}
}

/// Last will message, published by the broker if the client disconnects unexpectedly.
public struct Will: Sendable {
    public var topic: String
    public var payload: Data
    public var qos: MQTTQoS
    public var retain: Bool
    /// MQTT v5 will properties. Must be empty for v3.1.1.
    public var properties: Properties

    public init(topic: String, payload: Data, qos: MQTTQoS = .atMostOnce, retain: Bool = false, properties: Properties = []) {
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain
        self.properties = properties
    }
}

public struct Identity: Sendable {
    /// MQTT client id sent in `CONNECT`. A v5 broker may assign a different one.
    public internal(set) var clientId: String
    /// MQTT username sent in `CONNECT`
    public let username: String?
    /// MQTT password sent in `CONNECT`
    public let password: String?
    public init(_ clientId: String = UUID().uuidString, username: String? = nil, password: String? = nil) {
        self.clientId = clientId
        self.username = username
        self.password = password
    }
}

/// MQTT v5 enhanced authentication step: receives the broker's `AUTH` challenge and returns the response.
public typealias Authflow = @Sendable (Auth) async throws -> Auth

/// Something that happened on a client, delivered through `MQTTClient.events()`.
public enum MQTTEvent: Sendable {
    case status(Status, previous: Status)
    case message(Message)
    case error(any Error)
}

/// Indicates the level of assurance for delivery of a packet.
public enum MQTTQoS: UInt8, Sendable {
    /// fire and forget
    case atMostOnce = 0
    /// wait for `PUBACK`
    case atLeastOnce = 1
    /// wait for `PUBREC`, send `PUBREL` and then wait for `PUBCOMP`
    case exactlyOnce = 2
}

public enum MQTTError: Sendable, Hashable, Swift.Error {
    /// client timed out while waiting for response from server
    case timeout
    /// client in not connect to the broker
    case unconnected
    /// Encode of MQTT packet error or invalid paarameters
    case packetError(Packet)
    /// Decode of MQTT message failed
    case decodeError(Decode)
    /// the server closed the connection
    case serverClose(ResultCode.Disconnect)
    /// user closed connectiion
    case clientClose(ResultCode.Disconnect)
    /// publish failed
    case publishFailed(ResultCode.Puback)
    /// We received an unexpected message while connecting
    /// result code may be  `ResultCode.ConnectV3` or `ResultCode.Connect` or `nil`
    case connectFailed(ResultCode? = nil)
    /// You called connect on a client that is already connected to the broker
    case alreadyOpened
    /// Client has already been closed
    case alreadyClosed
    /// Unexpected data packets were received
    case unexpectPacket
    /// Received unexpected message
    case unexpectMessage
    /// Invalid  certificate data when import tls cert file
    case invalidCertData
    /// Packet error incomplete packet
    case incompletePacket
    /// Auth packets sent without authWorkflow being supplied
    case authflowRequired
}
extension MQTTError {
    /// Errors generated by bad packets sent by the client
    public enum Packet: Sendable, Hashable {
        /// A parameter is invalid, or only valid for MQTT v5
        case badParameter
        /// QoS is not accepted by this connection as it is greater than the accepted value
        case qosInvalid
        /// publish messages on this connection do not support the retain flag
        case retainUnavailable
        /// subscribe/unsubscribe packet requires at least one topic
        case atLeastOneTopicRequired
        /// topic alias is greater than server maximum topic alias or the alias is zero
        case topicAliasOutOfRange
        /// invalid topic name
        case invalidTopicName
        /// client to server publish packets cannot include a subscription identifier
        case publishIncludesSubscription
    }
}

extension MQTTError {
    public enum Decode: Sendable, Hashable {
        /// Read variable length overflow
        case varintOverflow
        /// Reader stream completed
        case streamIsComplete
        /// Packet received contained invalid tokens
        case unexpectedTokens
        /// got unexpected data length when decoder read
        case unexpectedDataLength
        /// Failed to recognise the packet control type
        case unrecognisedPacketType
    }
}
extension MQTTError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .timeout:
            return "MQTTError.timeout"
        case .unconnected:
            return "MQTTError.unconnected"
        case .packetError(let packet):
            return "MQTTError.packetError(\(packet))"
        case .decodeError(let decode):
            return "MQTTError.decodeError(\(decode))"
        case .serverClose(let disconnect):
            return "MQTTError.serverClose(\(disconnect))"
        case .clientClose(let disconnect):
            return "MQTTError.clientClose(\(disconnect))"
        case .publishFailed(let puback):
            return "MQTTError.publishFailed(\(puback))"
        case .connectFailed(let resultCode):
            return "MQTTError.connectFailed(\(resultCode.map { "\($0)" } ?? "nil"))"
        case .alreadyOpened:
            return "MQTTError.alreadyOpened"
        case .alreadyClosed:
            return "MQTTError.alreadyClosed"
        case .unexpectPacket:
            return "MQTTError.unexpectPacket"
        case .unexpectMessage:
            return "MQTTError.unexpectMessage"
        case .invalidCertData:
            return "MQTTError.invalidCertData"
        case .incompletePacket:
            return "MQTTError.incompletePacket"
        case .authflowRequired:
            return "MQTTError.authflowRequired"
        }
    }
}
