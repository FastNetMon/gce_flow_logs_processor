package main

import (
	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/fastnetmon/gce_flow_logs_processor/fastnetmon"
	"log"
	"net"
	"strconv"
	"zombiezen.com/go/capnproto2"
)

// Example run: ./gce_flow_logs_processor -topic_id flow_logs_eu_west_2
// "jsonPayload":{"bytes_sent":"4004","connection":{"dest_ip":"86.184.83.214","dest_port":37332,"protocol":6,"src_ip":"10.154.0.2","src_port":22}

type GoogleConnection struct {
	Protocol        int    `json:"protocol"`
	DestinationIP   string `json:"dest_ip"`
	DestinationPort int    `json:"dest_port"`
	SourceIP        string `json:"src_ip"`
	SourcePort      int    `json:"src_port"`
}

type GooglePayload struct {
	BytesSent   string           `json:"bytes_sent"`
	PacketsSent string           `json:"packets_sent"`
	Connection  GoogleConnection `json:"connection"`
}

type GoogleFlow struct {
	Payload GooglePayload `json:"jsonPayload"`
}

var projectIdArgument = flag.String("subscription_project_id", "", "Project id")
var topicIdArgument = flag.String("topic_id", "", "Topic ID")
var teraFlowtargetAddress = flag.String("tera_flow_server", "127.0.0.1:8104", "Target server to expor tdata in Tera Flow format")

func main() {
	flag.Parse()

	ctx := context.Background()

	projectID := ""

	// Try getting projectID from CLI
	if *projectIdArgument == "" {

		if metadata.OnGCE() {
			var err error
			projectID, err = metadata.ProjectID()

			if err != nil {
				log.Fatalf("Please specify project_id in configuration. Cannot retrieve it from GCE: %v", err)
			}

			log.Printf("Will use project-id from GCE metadata service: %s", projectID)
		} else {
			log.Fatal("Please provide projectid via command line arguments")
		}
	} else {
		projectID = *projectIdArgument
		log.Printf("Will use project-id from command line: %s", projectID)
	}

	if *topicIdArgument == "" {
		log.Fatal("Please specify topic id via command line argument")
	}

	if *teraFlowtargetAddress == "" {
		log.Fatal("Please speciay target address")
	}

	log.Printf("Will export data to %s", *teraFlowtargetAddress)

	s, err := net.ResolveUDPAddr("udp4", *teraFlowtargetAddress)

	if err != nil {
		log.Fatalf("Cannot resolve address: %v", err)
	}

	tera_flow_client, err := net.DialUDP("udp4", nil, s)

	if err != nil {
		log.Fatalf("Cannot dial address: %v", err)
	}

	log.Printf("Topic id: %s", *topicIdArgument)

	// Creates a client.
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	subscription := client.Subscription("fastnetmon-instance-5")

	sub_exists, err := subscription.Exists(ctx)

	if err != nil {
		log.Fatalf("Cannot check subscription existance: %v", err)
	}

	if !sub_exists {
		log.Printf("Create subscription")

		subscription, err = client.CreateSubscription(context.Background(), "fastnetmon-instance-5",
			pubsub.SubscriptionConfig{Topic: client.Topic(*topicIdArgument)})

		if err != nil {
			log.Fatalf("Cannot subscribe to topic: %v", err)
		}
	}

	err = subscription.Receive(context.Background(), func(ctx context.Context, m *pubsub.Message) {
		log.Printf("Got message: %s", m.Data)

		googleFlow := GoogleFlow{}

		if err := json.Unmarshal([]byte(m.Data), &googleFlow); err != nil {
			log.Fatal(err)
		}

		log.Printf("Data: %+v", googleFlow.Payload)

		flow_log_into_capnp(&googleFlow.Payload, tera_flow_client)

		m.Ack()
	})

	if err != nil {
		log.Fatalf("Cannot consume stream", err)
	}
}

func flow_log_into_capnp(flow_log *GooglePayload, client *net.UDPConn) error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return fmt.Errorf("Cannot create new message: %w", err)
	}

	simple_packet, err := fastnetmon.NewRootSimplePacketType(seg)
	if err != nil {
		return fmt.Errorf("Cannot create capnp type: %w", err)
	}

	dst_ip := net.ParseIP(flow_log.Connection.DestinationIP)

	// Skip when we cannot parse IP
	if dst_ip == nil {
		return fmt.Errorf("Cannot decode destination IP: %v", flow_log.Connection.DestinationIP)
	}

	// Ignore IPv6
	if dst_ip.To4() == nil {
		return fmt.Errorf("Destionation IP is not IPv4: %v", flow_log.Connection.DestinationIP)
	}

	simple_packet.SetDstIp(ip2int(dst_ip))

	src_ip := net.ParseIP(flow_log.Connection.SourceIP)

	// Return when we cannot parse
	if src_ip == nil {
		return fmt.Errorf("Cannot decode source ip: %v", flow_log.Connection.SourceIP)
	}

	// Ignore IPv6 for now
	if src_ip.To4() == nil {
		return fmt.Errorf("Source IP is not IPv4: %v", flow_log.Connection.SourceIP)
	}

	simple_packet.SetSrcIp(ip2int(src_ip))

	simple_packet.SetSourcePort(uint16(flow_log.Connection.SourcePort))
	simple_packet.SetDestinationPort(uint16(flow_log.Connection.DestinationPort))

	// FNM multiplies length by this value, we need to keep it non zero
	simple_packet.SetSampleRatio(1)

	simple_packet.SetIpProtocolVersion(4)
	simple_packet.SetProtocol(uint32(flow_log.Connection.Protocol))

	bytes_number, err := strconv.ParseInt(flow_log.BytesSent, 10, 64)

	if err != nil {
		return fmt.Errorf("Cannot decode bytes: %w", err)
	}

	simple_packet.SetLength(uint64(bytes_number))

	packets_number, err := strconv.ParseInt(flow_log.PacketsSent, 10, 64)

	if err != nil {
		return fmt.Errorf("Cannot decode packets: %w", err)
	}

	simple_packet.SetNumberOfPackets(uint64(packets_number))

	// Write the message to UDP socket
	err = capnp.NewEncoder(client).Encode(msg)
	if err != nil {
		return fmt.Errorf("Cannot encode message to capnp: %w", err)
	}

	return nil
}

// It convers into little endian represenataion
// We use little endian in tera flow
func ip2int(ip net.IP) uint32 {
	if len(ip) == 16 {
		return binary.LittleEndian.Uint32(ip[12:16])
	}
	return binary.LittleEndian.Uint32(ip)
}
