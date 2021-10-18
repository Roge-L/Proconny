/*
 * This code is provided solely for the personal and private use of students
 * taking the CSC369H course at the University of Toronto. Copying for purposes
 * other than this use is expressly prohibited. All forms of distribution of
 * this code, including but not limited to public repositories on GitHub,
 * GitLab, Bitbucket, or any other online platform, whether as given or with
 * any changes, are expressly prohibited.
 *
 * Authors: Alexey Khrabrov, Andrew Pelegris, Karen Reid
 *
 * All of the files in this directory and all subdirectories are:
 * Copyright (c) 2019, 2020 Karen Reid
 */

/**
 * CSC369 Assignment 2 - Message queue implementation.
 *
 * You may not use the pthread library directly. Instead you must use the
 * functions and types available in sync.h.
 */

// NOTES TO SELF:
// - USE HELGRIND
// - USE ERRORS.C (REPORT ERROR, REPORT INFO)
// - USE RING_BUFFER PEEK
// - FAMILIARIZE KERNEL LL
// - USE SYNC FUNCS IN SYNC.C
// - USE COND_AUTO_WAKEUP for DEBUGGING
// - USE NDEBUG FLAG
// - KERNEL LL for POLL WAIT QUEUE
// - compile multiprod with NDEBUG when timing

#include <assert.h>
#include <errno.h>
#include <stdlib.h>

#include "errors.h"
#include "list.h"
#include "msg_queue.h"
#include "ring_buffer.h"

#include <stdio.h>

// Node in a linked list
typedef struct node_t {
	int events;
	int revents;
	int *event_triggered;
	cond_t *event_occurred;
	mutex_t *event_occurred_lock;
	list_entry lst_entry;
} node_t;

typedef struct concurrent_list {
	// if list is not created, we don't care, do the normal
	// read/write/close
	int list_created;
	list_head the_list;
} conc_list;

// Message queue implementation backend
typedef struct mq_backend {
	// Ring buffer for storing the messages
	ring_buffer buffer;

	// Reference count
	size_t refs;

	// Number of handles open for reads
	size_t readers;
	// Number of handles open for writes
	size_t writers;

	// Set to true when all the reader handles have been closed. Starts false
	// when they haven't been opened yet.
	bool no_readers;
	// Set to true when all the writer handles have been closed. Starts false
	// when they haven't been opened yet.
	bool no_writers;

	mutex_t lock;
	cond_t non_empty, empty;
	int revents;
	conc_list waitlist;

} mq_backend;

static int mq_init(mq_backend *mq, size_t capacity)
{
	if (ring_buffer_init(&mq->buffer, capacity) < 0) {
		return -1;
	}

	mq->refs = 0;

	mq->readers = 0;
	mq->writers = 0;

	mq->no_readers = false;
	mq->no_writers = false;

	mutex_init(&mq->lock);

	cond_init(&mq->empty);
	cond_init(&mq->non_empty);

	mutex_lock(&mq->lock);

	mq->revents = 0;

	mq->waitlist.list_created = 0;
	list_init(&mq->waitlist.the_list);

	mutex_unlock(&mq->lock);

	return 0;
}

static void mq_destroy(mq_backend *mq)
{
	assert(mq->refs == 0);
	assert(mq->readers == 0);
	assert(mq->writers == 0);

	ring_buffer_destroy(&mq->buffer);

	cond_destroy(&mq->empty);
	cond_destroy(&mq->non_empty);

	list_destroy(&mq->waitlist.the_list);
	
	mutex_destroy(&mq->lock);
}


#define ALL_FLAGS (MSG_QUEUE_READER | MSG_QUEUE_WRITER | MSG_QUEUE_NONBLOCK)

// Message queue handle is a combination of the pointer to the queue backend and
// the handle flags. The pointer is always aligned on 8 bytes - its 3 least
// significant bits are always 0. This allows us to store the flags within the
// same word-sized value as the pointer by ORing the pointer with the flag bits.

// Get queue backend pointer from the queue handle
static mq_backend *get_backend(msg_queue_t queue)
{
	mq_backend *mq = (mq_backend*)(queue & ~ALL_FLAGS);
	assert(mq);
	return mq;
}

// Get handle flags from the queue handle
static int get_flags(msg_queue_t queue)
{
	return (int)(queue & ALL_FLAGS);
}

// Create a queue handle for given backend pointer and handle flags
static msg_queue_t make_handle(mq_backend *mq, int flags)
{
	assert(((uintptr_t)mq & ALL_FLAGS) == 0);
	assert((flags & ~ALL_FLAGS) == 0);
	return (uintptr_t)mq | flags;
}


static msg_queue_t mq_open(mq_backend *mq, int flags)
{
	++mq->refs;

	if (flags & MSG_QUEUE_READER) {
		++mq->readers;
		mq->no_readers = false;
	}
	if (flags & MSG_QUEUE_WRITER) {
		++mq->writers;
		mq->no_writers = false;
	}

	return make_handle(mq, flags);
}

// Returns true if this was the last handle
static bool mq_close(mq_backend *mq, int flags)
{
	assert(mq->refs != 0);
	assert(mq->refs >= mq->readers);
	assert(mq->refs >= mq->writers);

	if ((flags & MSG_QUEUE_READER) && (--mq->readers == 0)) {
		mq->no_readers = true;
	}
	if ((flags & MSG_QUEUE_WRITER) && (--mq->writers == 0)) {
		mq->no_writers = true;
	}

	if (--mq->refs == 0) {
		assert(mq->readers == 0);
		assert(mq->writers == 0);
		return true;
	}
	return false;
}


msg_queue_t msg_queue_create(size_t capacity, int flags)
{
	if (flags & ~ALL_FLAGS) {
		errno = EINVAL;
		report_error("msg_queue_create");
		return MSG_QUEUE_NULL;
	}

	// Refuse to create a message queue without capacity for
	// at least one message (length + 1 byte of message data).
	if ( capacity < (sizeof(size_t) + 1) ) {
		errno = EINVAL;
		report_error("msg_queue_create");
		return MSG_QUEUE_NULL;
	}
	
	mq_backend *mq = (mq_backend*)malloc(sizeof(mq_backend));
	if (!mq) {
		report_error("malloc");
		return MSG_QUEUE_NULL;
	}
	// Result of malloc() is always aligned on 8 bytes, allowing us to use the
	// 3 least significant bits of the handle to store the 3 bits of flags
	assert(((uintptr_t)mq & ALL_FLAGS) == 0);

	if (mq_init(mq, capacity) < 0) {
		// Preserve errno value that can be changed by free()
		int e = errno;
		free(mq);
		errno = e;
		return MSG_QUEUE_NULL;
	}

	return mq_open(mq, flags);
}

msg_queue_t msg_queue_open(msg_queue_t queue, int flags)
{
	if (!queue) {
		errno = EBADF;
		report_error("msg_queue_open");
		return MSG_QUEUE_NULL;
	}

	if (flags & ~ALL_FLAGS) {
		errno = EINVAL;
		report_error("msg_queue_open");
		return MSG_QUEUE_NULL;
	}

	mq_backend *mq = get_backend(queue);

	//TODO: add necessary synchronization
	mutex_lock(&mq->lock);

	msg_queue_t new_handle = mq_open(mq, flags);

	mutex_unlock(&mq->lock);

	return new_handle;
}

int msg_queue_close(msg_queue_t *queue)
{
	if (!queue || !*queue) {
		errno = EBADF;
		report_error("msg_queue_close");
		return -1;
	}

	mq_backend *mq = get_backend(*queue);

	mutex_lock(&mq->lock);

	if (mq_close(mq, get_flags(*queue))) {
		
		mutex_unlock(&mq->lock);

		// Closed last handle; destroy the queue
		mq_destroy(mq);
		free(mq);

		*queue = MSG_QUEUE_NULL;
		return 0;
	}

	// if this is the last reader (or writer) handle, notify all the writer
	// (or reader) threads currently blocked in msg_queue_write() (or
	// msg_queue_read()) and msg_queue_poll() calls for this queue.
	if (mq->no_readers) {

		if (mq->waitlist.list_created) {
			struct list_entry* cur_lst_entry;
			node_t *cur;
			list_for_each(cur_lst_entry, &mq->waitlist.the_list) {
				cur = container_of(cur_lst_entry, node_t, lst_entry);
				if (cur->events & MQPOLL_NOREADERS) {
					
					cur->revents = cur->revents | MQPOLL_NOREADERS;
					*cur->event_triggered = 1;
					mutex_lock(cur->event_occurred_lock);
					cond_signal(cur->event_occurred);
					mutex_unlock(cur->event_occurred_lock);
				}
			}
		} else {
			mq->revents = mq->revents | MQPOLL_NOREADERS;
		}

		cond_broadcast(&(mq->empty));
		mutex_unlock(&mq->lock);

		*queue = MSG_QUEUE_NULL;
		return 0;
	} else if (mq->no_writers) {

		if (mq->waitlist.list_created) {
			struct list_entry* cur_lst_entry;
			node_t *cur;
			list_for_each(cur_lst_entry, &mq->waitlist.the_list) {
				cur = container_of(cur_lst_entry, node_t, lst_entry);
				if ((cur->events & MQPOLL_NOWRITERS) || (cur->events & MQPOLL_READABLE)) {
					
					cur->revents = cur->revents | MQPOLL_NOWRITERS;
					*cur->event_triggered = 1;
					mutex_lock(cur->event_occurred_lock);
					cond_signal(cur->event_occurred);
					mutex_unlock(cur->event_occurred_lock);
				}
			}
		} else {
			mq->revents = mq->revents | MQPOLL_NOWRITERS;
		}

		cond_broadcast(&(mq->non_empty));
		mutex_unlock(&mq->lock);

		*queue = MSG_QUEUE_NULL;
		return 0;
	}

	mutex_unlock(&mq->lock);

	*queue = MSG_QUEUE_NULL;
	return 0;
}


ssize_t msg_queue_read(msg_queue_t queue, void *buffer, size_t length)
{
	// Check if the queue handle has read permissions
	if (!(get_flags(queue) & MSG_QUEUE_READER)) {
		errno = EBADF;
		report_error("msg_queue_read");
		return -1;
	}

	mq_backend *mq = get_backend(queue);

	mutex_lock(&mq->lock);

	// Check if the queue is empty
	while (ring_buffer_used(&mq->buffer) == 0) {

		// Return error if in non-blocking mode
		if (get_flags(queue) & MSG_QUEUE_NONBLOCK) {
			mutex_unlock(&mq->lock);
			errno = EAGAIN;
			report_info("msg_queue_read");
			return -1;
		}

		// Return 0 if the queue is empty and all the writer
		// handles to the queue have been closed
		if (mq->no_writers) {
			if (mq->waitlist.list_created) {
				struct list_entry* cur_lst_entry;
				node_t *cur;
				list_for_each(cur_lst_entry, &mq->waitlist.the_list) {
					cur = container_of(cur_lst_entry, node_t, lst_entry);
					if ((cur->events & MQPOLL_READABLE) || (cur->events & MQPOLL_NOWRITERS)) {
						
						cur->revents = cur->revents | MQPOLL_NOWRITERS;
						*cur->event_triggered = 1;
						mutex_lock(cur->event_occurred_lock);
						cond_signal(cur->event_occurred);
						mutex_unlock(cur->event_occurred_lock);
					}
				}
			} else {
				mq->revents = mq->revents | MQPOLL_NOWRITERS;
			}
			return 0;
		}

		// Wait until the queue becomes non-empty
		cond_wait(&mq->non_empty, &mq->lock);
	}

	// Check if the buffer is too small
	if (!(ring_buffer_peek(&mq->buffer, buffer, length))) {
		mutex_unlock(&mq->lock);
		errno = EMSGSIZE;
		report_info("msg_queue_read");
		return -length;
	}

	if (!(ring_buffer_read(&mq->buffer, buffer, length))) {

		mutex_unlock(&mq->lock);
		report_error("msg_queue_read");
		return -1;
	}

	if (mq->waitlist.list_created) {
		struct list_entry* cur_lst_entry;
		node_t *cur;
		list_for_each(cur_lst_entry, &mq->waitlist.the_list) {
			cur = container_of(cur_lst_entry, node_t, lst_entry);
			if (cur->events & MQPOLL_WRITABLE) {
				
				cur->revents = cur->revents | MQPOLL_WRITABLE;
				*cur->event_triggered = 1;
				mutex_lock(cur->event_occurred_lock);
				cond_signal(cur->event_occurred);
				mutex_unlock(cur->event_occurred_lock);
			}
		}
	} else {
		mq->revents = mq->revents | MQPOLL_WRITABLE;
	}

	cond_signal(&mq->empty);

	mutex_unlock(&mq->lock);
	return length;
}

int msg_queue_write(msg_queue_t queue, const void *buffer, size_t length)
{
	// Check if the length of the message is zero
	if (length == 0) {
		errno = EINVAL;
		report_error("msg_queue_write");
		return -1;
	}

	// Check if the queue handle has write permissions
	if (!(get_flags(queue) & MSG_QUEUE_WRITER)) {
		errno = EBADF;
		report_error("msg_queue_write");
		return -1;
	}

	mq_backend *mq = get_backend(queue);

	mutex_lock(&mq->lock);

	// Fail if all the reader handles to the queue have been closed
	if (mq->no_readers) {

		if (mq->waitlist.list_created) {
			struct list_entry* cur_lst_entry;
			node_t *cur;
			list_for_each(cur_lst_entry, &mq->waitlist.the_list) {
				cur = container_of(cur_lst_entry, node_t, lst_entry);
				if (cur->events & MQPOLL_NOREADERS) {
					
					cur->revents = cur->revents | MQPOLL_NOREADERS;
					*cur->event_triggered = 1;
					mutex_lock(cur->event_occurred_lock);
					cond_signal(cur->event_occurred);
					mutex_unlock(cur->event_occurred_lock);
				}
			}
		} else {
			mq->revents = mq->revents | MQPOLL_NOREADERS;
		}

		mutex_unlock(&mq->lock);
		errno = EPIPE;
		report_info("msg_queue_write");
		return -1;
	}

	// Check if the entire capacity of the queue is enough for the 
	// incoming message
	if (mq->buffer.size < length) {
		mutex_unlock(&mq->lock);
		errno = EMSGSIZE;
		report_info("msg_queue_write");
		return -1;
	}

	// Check if the queue currently has enough space for the incoming 
	// message
	while (ring_buffer_free(&mq->buffer) < length + sizeof(size_t)) {

		// Return error if in non-blocking mode
		if (get_flags(queue) & MSG_QUEUE_NONBLOCK) {
			mutex_unlock(&mq->lock);
			errno = EAGAIN;
			report_info("msg_queue_write");
			return -1;
		}

		// Wait until the queue becomes empty
		cond_wait(&mq->empty, &mq->lock);
	}

	if (!(ring_buffer_write(&mq->buffer, buffer, length))) {

		mutex_unlock(&mq->lock);
		errno = EMSGSIZE;
		report_info("msg_queue_read");
		return -1;
	}

	if (mq->waitlist.list_created) {
		struct list_entry* cur_lst_entry;
		node_t *cur;
		list_for_each(cur_lst_entry, &mq->waitlist.the_list) {
			cur = container_of(cur_lst_entry, node_t, lst_entry);
			if (cur->events & MQPOLL_READABLE) {
				
				cur->revents = cur->revents | MQPOLL_READABLE;
				*cur->event_triggered = 1;
				mutex_lock(cur->event_occurred_lock);
				cond_signal(cur->event_occurred);
				mutex_unlock(cur->event_occurred_lock);
			}
		}
	} else {
		mq->revents = mq->revents | MQPOLL_READABLE;
	}

	cond_signal(&mq->non_empty);
	
	mutex_unlock(&mq->lock);
	return 0;
}

#define MQPOLL_FLAGS (MQPOLL_READABLE | MQPOLL_WRITABLE | MQPOLL_NOREADERS | MQPOLL_NOWRITERS)

int msg_queue_poll(msg_queue_pollfd *fds, size_t nfds)
{
	// No events are subscribed to, i.e. nfds is 0
	if (nfds == 0) {
		errno = EINVAL;
		report_error("msg_queue_poll");
		return -1;
	}
	// or all the pollfd entries have the queue field set to MSG_QUEUE_NULL.
	bool no_subbed_events = false;
	for (int i = 0; i < (int)nfds; i++) {
		if ((i == ((int)nfds) - 1) && (fds[i].queue == MSG_QUEUE_NULL)) {
			no_subbed_events = true;
		}
		if (!(fds[i].queue == MSG_QUEUE_NULL)) {
			break;
		}
	}
	if (no_subbed_events) {
		errno = EINVAL;
		report_error("msg_queue_poll");
		return -1;
	}

	// events field in a pollfd entry is invalid, i.e. set to 0 or not a
	// valid combination of MQPOLL_* constants).
	for (int i = 0; i < (int)nfds; i++) {
		if (fds[i].events == 0 
			|| (fds[i].events & ~MQPOLL_FLAGS)
			|| ((fds[i].events & MQPOLL_READABLE) && (fds[i].events & MQPOLL_NOREADERS))
			|| ((fds[i].events & MQPOLL_WRITABLE) && (fds[i].events & MQPOLL_NOWRITERS))
			|| ((fds[i].events & MQPOLL_WRITABLE) && (fds[i].events & MQPOLL_NOREADERS))) {

				errno = EINVAL;
				report_error("msg_queue_poll");
				return -1;
		}
	}

	// MQPOLL_READABLE requested for a non-reader queue handle or
	// MQPOLL_WRITABLE requested for a non-writer queue handle.
	for (int i = 0; i < (int)nfds; i++) {
		if ((fds[i].events & MQPOLL_READABLE) && (get_flags(fds[i].queue) & ~MSG_QUEUE_READER)) {
			errno = EINVAL;
			report_error("msg_queue_poll");
			return -1;
		}
		if ((fds[i].events & MQPOLL_WRITABLE) && (get_flags(fds[i].queue) & ~MSG_QUEUE_WRITER)) {
			errno = EINVAL;
			report_error("msg_queue_poll");
			return -1;
		}
	}

	// Set all pollfd structure's revents integer to 0
	for (int i = 0; i < (int)nfds; i++) {
		fds[i].revents = 0;
	}

	// Dynamically create the event_occurred condition variable
	cond_t *event_occurred = (cond_t*)malloc(sizeof(cond_t));
	if (!event_occurred) {
		report_error("msg_queue_poll");
		return -1;
	}

	// Dynamically create the event_occurred_lock mutex
	mutex_t *event_occurred_lock = (mutex_t*)malloc(sizeof(mutex_t));
	if (!event_occurred_lock) {
		report_error("msg_queue_poll");
		return -1;
	}

	// Dynamically create the event_triggered integer
	int *event_triggered = (int*)malloc(sizeof(int));
	if (!event_triggered) {
		report_error("msg_queue_poll");
		return -1;
	}
	
	// Initialize the event_triggered integer
	*event_triggered = 0;

	int num_queues_ready = 0;
	// Create the waitlists, set the events in the queues
	for (int i = 0; i < (int)nfds; i++) {

		// Dynamically create this pollfd structure's node
		// that goes into the waitlist
		node_t *node = (node_t*)malloc(sizeof(node_t));
		if (!node) {
			report_error("msg_queue_poll");
			return -1;
		}

		// Set the event integers of the nodes correspondingly
		node->events = fds[i].events;
		node->revents = fds[i].revents;

		// Assign this node our dynamic event_occurred_lock mutex
		node->event_occurred_lock = event_occurred_lock;

		// Assign this node our dynamic event_triggered variable
		node->event_triggered = event_triggered;

		// Assign this node our dynamic event_occurred condition variable
		node->event_occurred = event_occurred;

		// Dynamically create this pollfd structure's node's list entry
		// and initialize it
		list_entry *lst_entry = (list_entry*)malloc(sizeof(list_entry));
		if (!lst_entry) {
			report_error("msg_queue_poll");
			return -1;
		}
		list_entry_init(&node->lst_entry);

		// Add this pollfd structure's node into the corresponding
		// mq_backend's waitlist
		mq_backend *mq = get_backend(fds[i].queue);
		mutex_lock(&mq->lock);

		// Add this node to the waitlist
		list_add_tail(&mq->waitlist.the_list, &node->lst_entry);

		mutex_unlock(&mq->lock);
	}

	// Checking if any events already returned from previous poll calls
	for (int i = 0; i < (int)nfds; i++) {
		struct list_entry* cur_lst_entry;
		node_t *cur;
		mq_backend *mq = get_backend(fds[i].queue);
		mutex_lock(&mq->lock);
		list_for_each(cur_lst_entry, &mq->waitlist.the_list) {
			cur = container_of(cur_lst_entry, node_t, lst_entry);
			if (mq->revents != 0) {
				cur->revents = cur->revents | mq->revents;
				*event_triggered = 1;
			}
		}
		mutex_unlock(&mq->lock);
	}


	// Acquire this poll call's CV lock before notifying event
	// operations that they can begin updating nodes
	// and most importantly signaling this poll call's
	// event_occurred CV
	mutex_lock(event_occurred_lock);

	// Notify all of the operations that want to access the waitlists
	// that they are now properly created and ready for use
	// NOTE: Not performed in the previous loop because there
	// may be multiple pollfd structures for the same queue
	for (int i = 0; i < (int)nfds; i++) {
		mq_backend *mq = get_backend(fds[i].queue);
		mutex_lock(&mq->lock);

		mq->waitlist.list_created = 1;
		
		mutex_unlock(&mq->lock);
	}

	// After acquiring the poll CV lock, AND THEN notifying
	// that the waitlist is accessible, we can call cond_wait
	// which will unlock the CV
	// QUESTION: what if two threads create pollfd for the
	// same queue?
	// ANSWER: one of them will have to get the signal b/c
	// the event op would be trying to get the 
	// event_occurred_lock which is currently held by the
	// poll thread
	while (!(*event_triggered)) {
		cond_wait(event_occurred, event_occurred_lock);
	}
	mutex_unlock(event_occurred_lock);

	// Update every pollfd structure's revents integer
	struct list_entry* cur_lst_entry;
	for (int i = 0; i < (int)nfds; i++) {
		node_t *cur;
		mq_backend *mq = get_backend(fds[i].queue);
		mutex_lock(&mq->lock);
		list_for_each(cur_lst_entry, &mq->waitlist.the_list) {
			cur = container_of(cur_lst_entry, node_t, lst_entry);
			if (((fds[i].events & MQPOLL_READABLE) && (cur->revents & MQPOLL_READABLE))
				|| ((fds[i].events & MQPOLL_WRITABLE) && (cur->revents & MQPOLL_WRITABLE))
				|| ((fds[i].events & MQPOLL_NOREADERS) && (cur->revents & MQPOLL_NOREADERS))
				|| ((fds[i].events & MQPOLL_NOWRITERS) && (cur->revents & MQPOLL_NOWRITERS))) {
				
				fds[i].revents = fds[i].revents | cur->revents;
				cur->events = 0;
				cur->revents = 0;
				mq->revents = mq->revents & ~fds[i].revents;
				num_queues_ready += 1;
				*event_triggered = 0;
			}
		}

		// Delete and free the waitlist
		// Go through the waitlist and free dynamic memory
		// free(event_occurred);
		// free(event_occurred_lock);
		// free(event_triggered);
		struct list_entry* cur_lst_entry;
		for (int i = 0; i < (int)nfds; i++) {
			mq->waitlist.list_created = 0;
			list_for_each(cur_lst_entry, &mq->waitlist.the_list) {
				cur = container_of(cur_lst_entry, node_t, lst_entry);
				if (cur->events == 0 && cur->revents == 0) {
					// free(cur_lst_entry);
					// free(cur);
				}
			}
		}
		mutex_unlock(&mq->lock);
	}

	return num_queues_ready;
}
