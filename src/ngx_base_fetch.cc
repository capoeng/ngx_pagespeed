/*
 * Copyright 2012 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Author: jefftk@google.com (Jeff Kaufman)

#include "ngx_base_fetch.h"

#include "ngx_pagespeed.h"

#include "net/instaweb/http/public/response_headers.h"
#include "net/instaweb/rewriter/public/rewrite_stats.h"
#include "net/instaweb/util/public/google_message_handler.h"
#include "net/instaweb/util/public/message_handler.h"

namespace net_instaweb {
	
ngx_connection_t *NgxBaseFetch::pipe_conn_ = NULL;
int NgxBaseFetch::pipefds_[2] = {-1, -1};

	
// handle base fetch event(Flush/HeadersComplete/Done),
// and ignore the event corresponding to SKIP.
// SKIP should only be specified in ps_release_request_context,
// so that released base fetch event will be ignored.
//
// modified from ngx_channel_handler
void NgxBaseFetch::clear_pipe(ngx_http_request_t *skip = NULL) {
  for ( ;; ) {
	ngx_http_request_t *requests[512];
	ssize_t size = read(pipe_conn_->fd,
		 static_cast<void *>(requests), sizeof(requests));

	if (size == -1) {
	  if (ngx_errno == EINTR) {
		continue;
	  } else if (ngx_errno == EAGAIN) {
		return;
	  }
	}

	if (size <= 0) {
	  // Terminate
	  if (ngx_event_flags & NGX_USE_EPOLL_EVENT) {
		ngx_del_conn(pipe_conn_, 0);
	  }

	  ngx_close_connection(pipe_conn_);
	  pipe_conn_ = NULL;
	  break;
	}

	ngx_uint_t i;
	for (i = 0; i < size / sizeof(requests[0]); i++) {
	  ngx_http_request_t *r = requests[i];
	  if (r == skip) {
		continue;
	  }

	  ngx_http_finalize_request(r, response_handler(r));
	}
  }
}

void NgxBaseFetch::event_handler(ngx_event_t *ev) {
  if (ev->timedout) {
	ev->timedout = 0;
	return;
  }
  ngx_log_debug0(NGX_LOG_DEBUG_EVENT, ev->log, 0, "base fetch event handler");
  clear_pipe(NULL);
}

ngx_int_t NgxBaseFetch::init(ngx_cycle_t *cycle) {
  ps_main_conf_t* cfg_m = static_cast<ps_main_conf_t*>(
		 ngx_http_cycle_get_module_main_conf(cycle, ngx_pagespeed));

  if (pipefds_[0] != -1) {
    return NGX_OK;
  }
  
  if (::pipe(pipefds_) != 0) {
	cfg_m->handler->Message(net_instaweb::kError, "base fetch pipe() failed");
	return NGX_ERROR;
  }

  if (ngx_nonblocking(pipefds_[0]) == -1) {
		cfg_m->handler->Message(net_instaweb::kError,
		"base fetch pipe[0] " ngx_nonblocking_n " failed");
	goto failed;
  }

  // following codes are modified from ngx_add_channel_event
  // we have to save ngx_connection_t, so can not use ngx_add_channel_event
  ngx_event_t *rev, *wev;
  ngx_connection_t *c;

  c = ngx_get_connection(pipefds_[0], cycle->log);

  if (c == NULL) {
	goto failed;
  }

  c->pool = cycle->pool;

  rev = c->read;
  wev = c->write;

  rev->log = cycle->log;
  wev->log = cycle->log;

#if (NGX_THREADS)
  rev->lock = &c->lock;
  wev->lock = &c->lock;
  rev->own_lock = &c->lock;
  wev->own_lock = &c->lock;
#endif

  rev->channel = 1;
  wev->channel = 1;

  rev->handler = NgxBaseFetch::event_handler;

  // only EPOLL event has both add_event and add_connection
  // same as ngx_add_channel_event
  if (ngx_add_conn && (ngx_event_flags & NGX_USE_EPOLL_EVENT) == 0) {
	if (ngx_add_conn(c) == NGX_ERROR) {
	  ngx_free_connection(c);
	  goto failed;
	}
  } else {
	if (ngx_add_event(rev, NGX_READ_EVENT, 0) == NGX_ERROR) {
	  ngx_free_connection(c);
	  goto failed;
	}
  }

  pipe_conn_ = c;
  return NGX_OK;
 failed:

  if (close(pipefds_[0]) == -1) {
	ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
		"close() base fetch pipe[0] failed");
  }
  if (close(pipefds_[1]) == -1) {
	ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
		"close() base fetch pipe[1] failed");
  }
  return NGX_ERROR;
}

void NgxBaseFetch::terminate(ngx_cycle_t *cycle) {
  if (pipe_conn_ == NULL) {
	return;
  }

  if (close(pipefds_[0]) == -1) {
	ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
		 "close() base fetch pipe[0] failed");
  }
  if (close(pipefds_[1]) == -1) {
	ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
		 "close() base fetch pipe[1] failed");
  }
}

void NgxBaseFetch::signal(ngx_http_request_t *r) {
  ssize_t size = 0;

  ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
				 "base fetch signal");

  while (true) {
	size = write(pipefds_[1], static_cast<void *>(&r),
		  sizeof(r));
	if (size == -1 && errno == EINTR) {
	  continue;
	}
	return;
  }
}

NgxBaseFetch::NgxBaseFetch(ngx_http_request_t* r,
                           NgxServerContext* server_context,
                           const RequestContextPtr& request_ctx)
    : AsyncFetch(request_ctx),
      request_(r),
      server_context_(server_context),
      done_called_(false),
      last_buf_sent_(false),
      references_(2),
      flush_(false) {
  if (pthread_mutex_init(&mutex_, NULL)) CHECK(0);
  PopulateRequestHeaders();
}

NgxBaseFetch::~NgxBaseFetch() {
  pthread_mutex_destroy(&mutex_);
}

void NgxBaseFetch::Lock() {
  pthread_mutex_lock(&mutex_);
}

void NgxBaseFetch::Unlock() {
  pthread_mutex_unlock(&mutex_);
}

void NgxBaseFetch::PopulateRequestHeaders() {
  CopyHeadersFromTable<RequestHeaders>(&request_->headers_in.headers,
                                       request_headers());
}

void NgxBaseFetch::PopulateResponseHeaders() {
  CopyHeadersFromTable<ResponseHeaders>(&request_->headers_out.headers,
                                        response_headers());

  response_headers()->set_status_code(request_->headers_out.status);

  // Manually copy over the content type because it's not included in
  // request_->headers_out.headers.
  response_headers()->Add(
      HttpAttributes::kContentType,
      ngx_psol::str_to_string_piece(request_->headers_out.content_type));

  // TODO(oschaaf): ComputeCaching should be called in setupforhtml()?
  response_headers()->ComputeCaching();
}

template<class HeadersT>
void NgxBaseFetch::CopyHeadersFromTable(ngx_list_t* headers_from,
                                        HeadersT* headers_to) {
  // http_version is the version number of protocol; 1.1 = 1001. See
  // NGX_HTTP_VERSION_* in ngx_http_request.h
  headers_to->set_major_version(request_->http_version / 1000);
  headers_to->set_minor_version(request_->http_version % 1000);

  // Standard nginx idiom for iterating over a list.  See ngx_list.h
  ngx_uint_t i;
  ngx_list_part_t* part = &headers_from->part;
  ngx_table_elt_t* header = static_cast<ngx_table_elt_t*>(part->elts);

  for (i = 0 ; /* void */; i++) {
    if (i >= part->nelts) {
      if (part->next == NULL) {
        break;
      }

      part = part->next;
      header = static_cast<ngx_table_elt_t*>(part->elts);
      i = 0;
    }

    StringPiece key = ngx_psol::str_to_string_piece(header[i].key);
    StringPiece value = ngx_psol::str_to_string_piece(header[i].value);

    headers_to->Add(key, value);
  }
}

bool NgxBaseFetch::HandleWrite(const StringPiece& sp,
                               MessageHandler* handler) {
  Lock();
  buffer_.append(sp.data(), sp.size());
  Unlock();
  return true;
}

ngx_int_t NgxBaseFetch::CopyBufferToNginx(ngx_chain_t** link_ptr) {
  if (done_called_ && last_buf_sent_) {
    // OK means HandleDone has been called
    *link_ptr = NULL;
    return NGX_OK;
  }

  int rc = ngx_psol::string_piece_to_buffer_chain(
      request_->pool, buffer_, link_ptr, done_called_ /* send_last_buf */);
  if (rc != NGX_OK) {
    return rc;
  }

  // Done with buffer contents now.
  buffer_.clear();

  if (done_called_) {
    last_buf_sent_ = true;
    return NGX_OK;
  }

  return NGX_AGAIN;
}

// There may also be a race condition if this is called between the last Write()
// and Done() such that we're sending an empty buffer with last_buf set, which I
// think nginx will reject.
ngx_int_t NgxBaseFetch::CollectAccumulatedWrites(ngx_chain_t** link_ptr) {
  ngx_int_t rc = NGX_DECLINED;
  Lock();
  if (flush_) {
    rc = CopyBufferToNginx(link_ptr);
    flush_ = false;
  }
  Unlock();
  if (rc == NGX_DECLINED) {
    *link_ptr = NULL;
  }
  return rc;
}

void NgxBaseFetch::RequestCollection() {
  Lock();
  if (flush_) {
    Unlock();
    return;
  }
  flush_ = true;
  ngx_psol::ps_base_fetch_signal(request_);
  Unlock();
  return;
}


ngx_int_t NgxBaseFetch::CollectHeaders(ngx_http_headers_out_t* headers_out) {
  Lock();
  const ResponseHeaders* pagespeed_headers = response_headers();
  Unlock();
  return ngx_psol::copy_response_headers_to_ngx(request_, *pagespeed_headers);
}

void NgxBaseFetch::HandleHeadersComplete() {
  // If this is a 404 response we need to count it in the stats.
  if (response_headers()->status_code() == HttpStatus::kNotFound) {
    server_context_->rewrite_stats()->resource_404_count()->Add(1);
  }

  RequestCollection();  // Headers available.
}

bool NgxBaseFetch::HandleFlush(MessageHandler* handler) {
  RequestCollection();  // A new part of the response body is available.
  return true;
}

void NgxBaseFetch::Release() {
  Lock();
  flush_ = true;
  Unlock();
  DecrefAndDeleteIfUnreferenced();
}

void NgxBaseFetch::DecrefAndDeleteIfUnreferenced() {
  // Creates a full memory barrier.
  if (__sync_add_and_fetch(&references_, -1) == 0) {
    delete this;
  }
}

void NgxBaseFetch::HandleDone(bool success) {
  // TODO(jefftk): it's possible that instead of locking here we can just modify
  // CopyBufferToNginx to only read done_called_ once.

  if (done_called_ == true) {
    return;
  }

  Lock();
  if (done_called_ == true) {
    Unlock();
    return;
  }

  done_called_ = true;
  if (!flush_) {
    flush_ = true;
    ngx_psol::ps_base_fetch_signal(request_);
  }

  Unlock();
  DecrefAndDeleteIfUnreferenced();
}

}  // namespace net_instaweb
