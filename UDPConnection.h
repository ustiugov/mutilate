#pragma once

#include <list>
#include <queue>

#include "Connection.h"
#include "ConnectionOptions.h"
#include "Generator.h"

class Buffer {
public:
	size_t size(void);
	void *pullup(size_t size);
	void drain(size_t size);
	ssize_t read_from_fd(int fd);
private:
	char data[4096];
	int idx = 0;
};

#include <iostream>

class OpQueue {
public:
	size_t size();
	void pop(Operation *op);
	void push(Operation op);
	Operation *find(uint16_t req_id);
private:
	std::list<Operation> list;
};

class UDPConnection : public Connection {
public:
	UDPConnection(struct event_base* base, string hostname, int port, options_t options, bool sampling);
	void drive_write_machine(double now = 0.0);
	void issue_something(double now);
	void issue_get(string *key, double now);
	void issue_get(Operation &op);
	void timer_callback(void);
	void read_callback(void);
	void pop_op(Operation *op);
	Operation *consume_udp_binary_response(Buffer *input);
private:
	int fd;
	uint16_t req_id = 0;
	KeyGenerator *keygen;
	double next_time;
	Generator *iagen;
	Buffer buffer;
	options_t options;
	struct event *timer;
	OpQueue op_queue;
	enum read_state_enum read_state;
	enum write_state_enum write_state;
};
