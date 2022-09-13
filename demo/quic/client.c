// Author: wangha <wangwei at emqx dot io>
//
// This software is supplied under the terms of the MIT License, a
// copy of which should be located in the distribution where this
// file was obtained (LICENSE.txt).  A copy of the license may also be
// found online at https://opensource.org/licenses/MIT.
//

//
// This is just a simple MQTT client demonstration application.
//
// The application has three sub-commands: `conn` `pub` and `sub`.
// The `conn` sub-command connects to the server.
// The `pub` sub-command publishes a given message to the server and then exits.
// The `sub` sub-command subscribes to the given topic filter and blocks
// waiting for incoming messages.
//
// # Example:
//
// Connect to the specific server:
// ```
// $ ./quic_client conn 'mqtt-quic://127.0.0.1:14567'
// ```
//
// Subscribe to `topic` and waiting for messages:
// ```
// $ ./quic_client sub 'mqtt-tcp://127.0.0.1:14567' topic
// ```
//
// Publish 'hello' to `topic`:
// ```
// $ ./quic_client pub 'mqtt-tcp://127.0.0.1:14567' topic hello
// ```
//

#include <nng/nng.h>
#include <nng/mqtt/mqtt_quic.h>
#include <nng/mqtt/mqtt_client.h>
#include <nng/supplemental/util/platform.h>

#include "msquic.h"

#include <stdio.h>
#include <stdlib.h>

#define CLIENT_SEND_Q_SZ 4

static nng_msg * send_q[CLIENT_SEND_Q_SZ];
static int send_q_pos = 0;
static int send_q_sz = 0;

static nng_socket * g_sock;

void
fatal(const char *msg, int rv)
{
	fprintf(stderr, "%s: %s\n", msg, nng_strerror(rv));
}

static inline void
put_send_q(nng_msg *msg)
{
	if (send_q_sz == 4) {
		printf("Msg Send Queue Overflow.\n");
		return;
	}
	send_q[send_q_pos] = msg;
	send_q_pos = (++send_q_pos) % CLIENT_SEND_Q_SZ;
	send_q_sz ++;
}

static inline nng_msg *
get_send_q()
{
	nng_msg *msg;
	if (send_q_sz == 0) {
		printf("Msg Send Queue Is Empty.\n");
		return NULL;
	}
	send_q_pos = (--send_q_pos) % CLIENT_SEND_Q_SZ;
	msg = send_q[send_q_pos];
	send_q_sz --;
	return msg;
}

static nng_msg *
mqtt_msg_compose(int type, int qos, char *topic, char *payload)
{
	// Mqtt connect message
	nng_msg *msg;
	nng_mqtt_msg_alloc(&msg, 0);

	if (type == 1) {
		nng_mqtt_msg_set_packet_type(msg, NNG_MQTT_CONNECT);

		nng_mqtt_msg_set_connect_keep_alive(msg, 60);
		nng_mqtt_msg_set_connect_clean_session(msg, false);
	} else if (type == 2) {
		nng_mqtt_msg_set_packet_type(msg, NNG_MQTT_SUBSCRIBE);

		int count = 1;

		nng_mqtt_topic_qos subscriptions[] = {
			{
				.qos   = qos,
				.topic = {
					.buf    = (uint8_t *) topic,
					.length = strlen(topic)
				}
			},
		};

		nng_mqtt_msg_set_subscribe_topics(msg, subscriptions, count);
	} else if (type == 3) {
		nng_mqtt_msg_set_packet_type(msg, NNG_MQTT_PUBLISH);

		nng_mqtt_msg_set_publish_dup(msg, 0);
		nng_mqtt_msg_set_publish_qos(msg, qos);
		nng_mqtt_msg_set_publish_retain(msg, 0);
		nng_mqtt_msg_set_publish_topic(msg, topic);
		nng_mqtt_msg_set_publish_payload(
		    msg, (uint8_t *) payload, strlen(payload));
	}

	return msg;
}

static int
connect_cb(void *rmsg, void * arg)
{
	printf("[Connected][%s]...\n", (char *)arg);

	nng_msg *msg;
	while (send_q_sz > 0) {
		msg = get_send_q();
		nng_sendmsg(*g_sock, msg, NNG_FLAG_ALLOC);
	}
	return 0;
}

static int
disconnect_cb(void *rmsg, void * arg)
{
	// printf("[Disconnected][%s]...\n", (char *)arg);
	printf("[Disconnected]...\n");
	return 0;
}

static int
msg_send_cb(void *rmsg, void * arg)
{
	printf("[Msg Sent][%s]...\n", (char *)arg);
	return 0;
}

static int
msg_recv_cb(void *rmsg, void * arg)
{
	printf("[Msg Arrived][%s]...\n", (char *)arg);
	nng_msg *msg = rmsg;
	uint32_t topicsz, payloadsz;

	char *topic   = (char *)nng_mqtt_msg_get_publish_topic(msg, &topicsz);
	char *payload = nng_mqtt_msg_get_publish_payload(msg, &payloadsz);

	printf("topic   => %.*s\n"
	       "payload => %.*s\n",topicsz, topic, payloadsz, payload);

	return (0);
}

static int
sqlite_config(nng_socket *sock, uint8_t proto_ver)
{
#if defined(NNG_SUPP_SQLITE)
	int rv;
	// create sqlite option
	nng_mqtt_sqlite_option *sqlite;
	if ((rv = nng_mqtt_alloc_sqlite_opt(&sqlite)) != 0) {
		fatal("nng_mqtt_alloc_sqlite_opt", rv);
	}
	// set sqlite option
	nng_mqtt_set_sqlite_enable(sqlite, true);
	nng_mqtt_set_sqlite_flush_threshold(sqlite, 10);
	nng_mqtt_set_sqlite_max_rows(sqlite, 20);
	nng_mqtt_set_sqlite_db_dir(sqlite, "/tmp/nanomq");

	// init sqlite db
	nng_mqtt_sqlite_db_init(sqlite, "mqtt_quic_client.db", proto_ver);

	// set sqlite option pointer to socket
	return nng_socket_set_ptr(*sock, NNG_OPT_MQTT_SQLITE, sqlite);
#else
	return (0);
#endif
}

//TODO remove before submitting PR
static void
send_message_interval(void *arg)
{
	nng_socket *sock = arg;
	nng_msg *   pub_msg;
	nng_mqtt_msg_alloc(&pub_msg, 0);

	nng_mqtt_msg_set_packet_type(pub_msg, NNG_MQTT_PUBLISH);
	nng_mqtt_msg_set_publish_topic(pub_msg, "topic123");
	nng_mqtt_msg_set_publish_payload(
	    pub_msg, (uint8_t *) "offline message", strlen("offline message"));

	for (;;) {
		nng_msleep(2000);
		nng_msg *dup_msg;
		nng_msg_dup(&dup_msg, pub_msg);
		nng_sendmsg(*sock, dup_msg, NNG_FLAG_ALLOC);
		printf("sending message\n");
	}
}

int
client(int type, const char *url, const char *qos, const char *topic, const char *data)
{
	nng_socket  sock;
	int         rv, sz, q;
	nng_msg *   msg;
	const char *arg = "CLIENT FOR QUIC";

	if ((rv = nng_mqtt_quic_client_open(&sock, url)) != 0) {
		printf("error in quic client open.\n");
	}
	if (0 != nng_mqtt_quic_set_connect_cb(&sock, connect_cb, (void *)arg) ||
	    0 != nng_mqtt_quic_set_disconnect_cb(&sock, disconnect_cb, (void *)arg) ||
	    0 != nng_mqtt_quic_set_msg_recv_cb(&sock, msg_recv_cb, (void *)arg) ||
	    0 != nng_mqtt_quic_set_msg_send_cb(&sock, msg_send_cb, (void *)arg)) {
		printf("error in quic client cb set.\n");
	}
	g_sock = &sock;

	sqlite_config(g_sock, MQTT_PROTOCOL_VERSION_v311);

	// MQTT Connect...
	msg = mqtt_msg_compose(1, 0, NULL, NULL);
	nng_sendmsg(sock, msg, NNG_FLAG_ALLOC);

	if (qos) {
		q = atoi(qos);
		if (q < 0 || q > 2) {
			printf("Qos should be in range(0~2).\n");
			q = 0;
		}
	}

	switch (type) {
	case 1:
		break;
	case 2:
		msg = mqtt_msg_compose(2, q, (char *)topic, NULL);
		put_send_q(msg);
		break;
	case 3:
		msg = mqtt_msg_compose(3, q, (char *)topic, (char *)data);
		put_send_q(msg);
		//TODO remove before submitting PR
		nng_thread *thread;
		nng_thread_create(&thread, send_message_interval, &sock);
		break;
	default:
		printf("Unknown command.\n");
	}

	for (;;)
		nng_msleep(1000);

	nng_close(sock);

	return (0);
}

static void
printf_helper(char *exec)
{
	fprintf(stderr, "Usage: %s conn <url>\n"
	                "       %s sub  <url> <qos> <topic>\n"
	                "       %s pub  <url> <qos> <topic> <data>\n", exec, exec, exec);
	exit(EXIT_FAILURE);
}

int
main(int argc, char **argv)
{
	int rc;
	memset(send_q, 0, sizeof(send_q));

	if (argc < 3)
		printf_helper(argv[0]);
	if (0 == strncmp(argv[1], "conn", 4) && argc == 3)
		client(1, argv[2], NULL, NULL, NULL);
	if (0 == strncmp(argv[1], "sub", 3)  && argc == 5)
		client(2, argv[2], argv[3], argv[4], NULL);
	if (0 == strncmp(argv[1], "pub", 3)  && argc == 6)
		client(3, argv[2], argv[3], argv[4], argv[5]);

	printf_helper(argv[0]);
	return 0;
}
