/**
 *    Copyright (C) 2017 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/s/async_requests_sender.h"

#include "mongo/client/remote_command_targeter.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"

namespace mongo {
namespace {

// Maximum number of retries for network and replication notMaster errors (per host).
const int kMaxNumFailedHostRetryAttempts = 3;

}  // namespace

// SAM: DANS here how?
AsyncRequestsSender::AsyncRequestsSender(OperationContext* opCtx,
                                         executor::TaskExecutor* executor,
                                         StringData dbName,
                                         const std::vector<AsyncRequestsSender::Request>& requests,
                                         const ReadPreferenceSetting& readPreference,
                                         Shard::RetryPolicy retryPolicy)
    : _opCtx(opCtx),
      _executor(executor),
      _db(dbName.toString()),
      _readPreference(readPreference),
      _retryPolicy(retryPolicy) {
    for (const auto& request : requests) {
        _remotes.emplace_back(request.shardId, request.cmdObj);
    }

    // Initialize command metadata to handle the read preference.
    _metadataObj = readPreference.toContainingBSON();

    // Schedule the requests immediately.

    // We must create the notification before scheduling any requests, because the notification is
    // signaled both on an error in scheduling the request and a request's callback.
    _notification.emplace();

    // We lock so that no callbacks signal the notification until after we are done scheduling
    // requests, to prevent signaling the notification twice, which is illegal.
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _scheduleRequests(lk);
}

// AsyncRequestsSender::AsyncRequestsSender(OperationContext* opCtx,
//                                          executor::TaskExecutor* executor,
//                                          StringData dbName,
//                                          const std::vector<std::pair<AsyncRequestsSender::Request,
//                                                                      AsyncRequestsSender::Request>>& requests,
//                                          const ReadPreferenceSetting& readPreference,
//                                          Shard::RetryPolicy retryPolicy)
//     : _opCtx(opCtx),
//       _executor(executor),
//       _db(dbName.toString()),
//       _readPreference(readPreference),
//       _retryPolicy(retryPolicy) {
//
//     // how does this translate to DANS
//     for (const auto& request : requests) {
//         _remotes.emplace_back(request.shardId, request.cmdObj);
//     }
//
//     // Initialize command metadata to handle the read preference.
//     _metadataObj = readPreference.toContainingBSON();
//
//     // Schedule the requests immediately.
//
//     // We must create the notification before scheduling any requests, because the notification is
//     // signaled both on an error in scheduling the request and a request's callback.
//     _notification.emplace();
//
//     // We lock so that no callbacks signal the notification until after we are done scheduling
//     // requests, to prevent signaling the notification twice, which is illegal.
//     stdx::lock_guard<stdx::mutex> lk(_mutex);
//     _scheduleRequests(lk);
// }

AsyncRequestsSender::~AsyncRequestsSender() {
    _cancelPendingRequests();

    // Wait on remaining callbacks to run.
    while (!done()) {
        next();
    }
}

AsyncRequestsSender::Response AsyncRequestsSender::next() {
    invariant(!done());

    // If needed, schedule requests for all remotes which had retriable errors.
    // If some remote had success or a non-retriable error, return it.
    boost::optional<Response> readyResponse;
    while (!(readyResponse = _ready())) {
        // Otherwise, wait for some response to be received.
        if (_interruptStatus.isOK()) {
            try {
                _notification->get(_opCtx);
            } catch (const AssertionException& ex) {
                // If the operation is interrupted, we cancel outstanding requests and switch to
                // waiting for the (canceled) callbacks to finish without checking for interrupts.
                _interruptStatus = ex.toStatus();
                _cancelPendingRequests();
                continue;
            }
        } else {
            _notification->get();
        }
    }
    return *readyResponse;
}

void AsyncRequestsSender::stopRetrying() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _stopRetrying = true;
}

bool AsyncRequestsSender::done() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    return std::all_of(
        _remotes.begin(), _remotes.end(), [](const RemoteData& remote) { return remote.done; });
}

void AsyncRequestsSender::_cancelPendingRequests() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _stopRetrying = true;

    // Cancel all outstanding requests so they return immediately.
    for (auto& remote : _remotes) {
        if (remote.cbHandle.isValid()) {
            _executor->cancel(remote.cbHandle);
        }
    }
}

boost::optional<AsyncRequestsSender::Response> AsyncRequestsSender::_ready() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    _notification.emplace();

    if (!_stopRetrying) {
        _scheduleRequests(lk);
    }

    // Check if any remote is ready.
    invariant(!_remotes.empty());
    for (auto& remote : _remotes) {
        if (remote.swResponse && !remote.done) {
            remote.done = true;
            if (remote.swResponse->isOK()) {
                invariant(remote.shardHostAndPort);
                return Response(std::move(remote.shardId),
                                std::move(remote.swResponse->getValue()),
                                std::move(*remote.shardHostAndPort));
            } else {
                // If _interruptStatus is set, promote CallbackCanceled errors to it.
                if (!_interruptStatus.isOK() &&
                    ErrorCodes::CallbackCanceled == remote.swResponse->getStatus().code()) {
                    remote.swResponse = _interruptStatus;
                }
                return Response(std::move(remote.shardId),
                                std::move(remote.swResponse->getStatus()),
                                std::move(remote.shardHostAndPort));
            }
        } else if (remote.DANSswResponses.first && !remote.primaryDone) {
            remote.primaryDone = true;
            // SAM: don't return here, only return when we have both responses
            if (!remote.DANSswResponses.first->isOK()) {
                // If _interruptStatus is set, promote CallbackCanceled errors to it.
                if (!_interruptStatus.isOK() &&
                    ErrorCodes::CallbackCanceled == remote.DANSswResponses.first->getStatus().code()) {
                    remote.swResponse = _interruptStatus;
                }
                const HostAndPort hp = remote.DANSHostAndPorts.get().first;
                return Response(std::move(remote.shardId),
                                (executor::RemoteCommandResponse)std::move(remote.DANSswResponses.first->getStatus()),
                                (HostAndPort)std::move(hp));
            }
        } else if (remote.DANSswResponses.second && !remote.secondaryDone) {
            remote.secondaryDone = true;
            if (!remote.DANSswResponses.second->isOK()) {
                // If _interruptStatus is set, promote CallbackCanceled errors to it.
                if (!_interruptStatus.isOK() &&
                    ErrorCodes::CallbackCanceled == remote.DANSswResponses.second->getStatus().code()) {
                    remote.swResponse = _interruptStatus;
                }
                const HostAndPort hp = remote.DANSHostAndPorts.get().second;
                return Response(std::move(remote.shardId),
                                (executor::RemoteCommandResponse)std::move(remote.DANSswResponses.second->getStatus()),
                                (HostAndPort)std::move(hp));
            }
        }
        if (remote.primaryDone && remote.secondaryDone) {
            invariant(remote.DANSHostAndPorts);
            // SAM: TODO: not quite right
            return Response(std::move(remote.shardId),
                            std::move(remote.swResponse->getValue()),
                            std::move(*remote.DANSHostAndPorts));
        }
    }
    // No remotes were ready.
    return boost::none;
}

// SAM: here is where they actually check
void AsyncRequestsSender::_scheduleRequests(WithLock lk) {
    invariant(!_stopRetrying);
    // Schedule remote work on hosts for which we have not sent a request or need to retry.
    for (size_t i = 0; i < _remotes.size(); ++i) {
        auto& remote = _remotes[i];

        // SAM: TODO: check both swResponse?
        // check to see if there's any response
          // if so handle either or both
        // otherwise, schedule another request

        // First check if the remote had a retriable error, and if so, clear its response field so
        // it will be retried.

        bool singleRemoteResponse = remote.swResponse && !remote.done;
        bool primaryRemoteResponse = remote.DANSswResponses.first && !remote.primaryDone;
        bool secondaryRemoteResponse = remote.DANSswResponses.second && !remote.secondaryDone;

        if (singleRemoteResponse) {
            // We check both the response status and command status for a retriable error.
            Status status = remote.swResponse->getStatus();
            if (status.isOK()) {
                // SAM: TODO: handle DANS related things here
                status = getStatusFromCommandResult(remote.swResponse->getValue().data);
            }

            if (!status.isOK()) {
                // There was an error with either the response or the command.
                auto shard = remote.getShard();
                if (!shard) {
                    remote.swResponse =
                        Status(ErrorCodes::ShardNotFound,
                               str::stream() << "Could not find shard " << remote.shardId);
                } else {
                    if (remote.shardHostAndPort) {
                        shard->updateReplSetMonitor(*remote.shardHostAndPort, status);
                    }
                    if (shard->isRetriableError(status.code(), _retryPolicy) &&
                        remote.retryCount < kMaxNumFailedHostRetryAttempts) {
                        LOG(1) << "Command to remote " << remote.shardId << " at host "
                               << *remote.shardHostAndPort
                               << " failed with retriable error and will be retried "
                               << causedBy(redact(status));
                        ++remote.retryCount;
                        remote.swResponse.reset();
                    }
                }
            }
        }

        if (primaryRemoteResponse) {
            // We check both the response status and command status for a retriable error.
            Status status = remote.DANSswResponses.first->getStatus();
            if (status.isOK()) {
                status = getStatusFromCommandResult(remote.DANSswResponses.first->getValue().data);
            }

            if (!status.isOK()) {
                // There was an error with either the response or the command.
                auto shard = remote.getShard();
                if (!shard) {
                    remote.DANSswResponses.first =
                        Status(ErrorCodes::ShardNotFound,
                               str::stream() << "Could not find shard " << remote.shardId);
                } else {
                    if (remote.DANSHostAndPorts) {
                        shard->updateReplSetMonitor(remote.DANSHostAndPorts.get().first, status);
                    }
                    if (shard->isRetriableError(status.code(), _retryPolicy) &&
                        remote.primaryRetryCount < kMaxNumFailedHostRetryAttempts) {
                        LOG(1) << "Command to remote " << remote.shardId << " at host "
                               << remote.DANSHostAndPorts.get().first
                               << " failed with retriable error and will be retried "
                               << causedBy(redact(status));
                        ++remote.primaryRetryCount;
                        remote.DANSswResponses.first.reset();
                    }
                }
            }
        }

        if (secondaryRemoteResponse) {
            // We check both the response status and command status for a retriable error.
            Status status = remote.DANSswResponses.second->getStatus();
            if (status.isOK()) {
                status = getStatusFromCommandResult(remote.DANSswResponses.second->getValue().data);
            }

            if (!status.isOK()) {
                // There was an error with either the response or the command.
                auto shard = remote.getShard();
                if (!shard) {
                    remote.DANSswResponses.second =
                        Status(ErrorCodes::ShardNotFound,
                               str::stream() << "Could not find shard " << remote.shardId);
                } else {
                    if (remote.DANSHostAndPorts) {
                        shard->updateReplSetMonitor(remote.DANSHostAndPorts.get().second, status);
                    }
                    if (shard->isRetriableError(status.code(), _retryPolicy) &&
                        remote.secondaryRetryCount < kMaxNumFailedHostRetryAttempts) {
                        LOG(1) << "Command to remote " << remote.shardId << " at host "
                               << remote.DANSHostAndPorts.get().second
                               << " failed with retriable error and will be retried "
                               << causedBy(redact(status));
                        ++remote.secondaryRetryCount;
                        remote.DANSswResponses.second.reset();
                    }
                }
            }
        }


        // SAM: adapt for DANS?
        // If the remote does not have a response or pending request, schedule remote work for it.
        if (!remote.swResponse && !remote.cbHandle.isValid()) {
          // SAM: i is just the index here
            auto scheduleStatus = _scheduleRequest(lk, i);
            if (!scheduleStatus.isOK()) {

                remote.swResponse = std::move(scheduleStatus);
                // Signal the notification indicating the remote had an error (we need to do this
                // because no request was scheduled, so no callback for this remote will run and
                // signal the notification).
                if (!*_notification) {
                    _notification->set();
                }
            }
        }
    }
}

Status AsyncRequestsSender::_scheduleRequest(WithLock, size_t remoteIndex) {
    auto& remote = _remotes[remoteIndex];

    invariant(!remote.cbHandle.isValid());
    invariant(!remote.swResponse);

    // SAM: ayy, here we go!
    // but this is only grabbing one, we need to posility to specify one to avoid
    Status resolveStatus = remote.resolveShardIdToHostAndPort(_readPreference);
    if (!resolveStatus.isOK()) {
        return resolveStatus;
    }

    // SAM: if our read preference is for dual then we gotta do 2 here
    if (_readPreference.pref == ReadPreference::DuplicatePrimary) {
      executor::RemoteCommandRequest request1(
          remote.DANSHostAndPorts.get().first, _db, remote.cmdObj, _metadataObj, _opCtx);

      // SAM: TODO: make separate callbacks for populating each response
      auto callbackStatus1 = _executor->scheduleRemoteCommand(
          request1, [=](const executor::TaskExecutor::RemoteCommandCallbackArgs& cbData) {
              _DANShandleResponse(cbData, remoteIndex, true); // bool is isPrimary
          });
      if (!callbackStatus1.isOK()) {
          return callbackStatus1.getStatus();
      }

      remote.cbHandles.first = callbackStatus1.getValue();

      executor::RemoteCommandRequest request2(
          remote.DANSHostAndPorts.get().second, _db, remote.cmdObj, _metadataObj, _opCtx);

      auto callbackStatus2 = _executor->scheduleRemoteCommand(
          request2, [=](const executor::TaskExecutor::RemoteCommandCallbackArgs& cbData) {
              _DANShandleResponse(cbData, remoteIndex, false); // bool is isPrimary
          });

      // SAM: TODO: only error if one breaks?
      if (!callbackStatus2.isOK()) {
          return callbackStatus2.getStatus();
      }

      remote.cbHandles.second = callbackStatus2.getValue();

    } else {
      executor::RemoteCommandRequest request(
          *remote.shardHostAndPort, _db, remote.cmdObj, _metadataObj, _opCtx);

      auto callbackStatus = _executor->scheduleRemoteCommand(
          request, [=](const executor::TaskExecutor::RemoteCommandCallbackArgs& cbData) {
              _handleResponse(cbData, remoteIndex);
          });
      if (!callbackStatus.isOK()) {
          return callbackStatus.getStatus();
      }

      remote.cbHandle = callbackStatus.getValue();
    }
    return Status::OK();
}

// SAM: TODO: do this -> god that's the worst comment I've ever written I need to get a grip
void AsyncRequestsSender::_DANShandleResponse(
    const executor::TaskExecutor::RemoteCommandCallbackArgs& cbData,
    size_t remoteIndex,
    bool isPrimary) {

    stdx::lock_guard<stdx::mutex> lk(_mutex);

    auto& remote = _remotes[remoteIndex];

    // split on isPrimary
    if (isPrimary) {
        invariant(!remote.DANSswResponses.first);

        // Clear the callback handle. This indicates that we are no longer waiting on a response from
        // 'remote'.
        remote.cbHandles.first = executor::TaskExecutor::CallbackHandle();

        // Store the response or error.
        if (cbData.response.status.isOK()) {
            remote.DANSswResponses.first = std::move(cbData.response);
        } else {
            remote.DANSswResponses.first = std::move(cbData.response.status);
        }
    } else {
        invariant(!remote.DANSswResponses.second);

        // Clear the callback handle. This indicates that we are no longer waiting on a response from
        // 'remote'.
        remote.cbHandles.second = executor::TaskExecutor::CallbackHandle();

        // Store the response or error.
        if (cbData.response.status.isOK()) {
            remote.DANSswResponses.second = std::move(cbData.response);
        } else {
            remote.DANSswResponses.second = std::move(cbData.response.status);
        }
    }

    // Signal the notification indicating that a remote received a response.
    if (!*_notification) {
        _notification->set();
    }
}


void AsyncRequestsSender::_handleResponse(
    const executor::TaskExecutor::RemoteCommandCallbackArgs& cbData, size_t remoteIndex) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    auto& remote = _remotes[remoteIndex];
    invariant(!remote.swResponse);

    // Clear the callback handle. This indicates that we are no longer waiting on a response from
    // 'remote'.
    remote.cbHandle = executor::TaskExecutor::CallbackHandle();

    // Store the response or error.
    if (cbData.response.status.isOK()) {
        remote.swResponse = std::move(cbData.response);
    } else {
        remote.swResponse = std::move(cbData.response.status);
    }

    // Signal the notification indicating that a remote received a response.
    if (!*_notification) {
        _notification->set();
    }
}

AsyncRequestsSender::Request::Request(ShardId shardId, BSONObj cmdObj)
    : shardId(shardId), cmdObj(cmdObj) {}

AsyncRequestsSender::Response::Response(ShardId shardId,
                                        executor::RemoteCommandResponse response,
                                        HostAndPort hp)
    : shardId(std::move(shardId)),
      swResponse(std::move(response)),
      shardHostAndPort(std::move(hp)) {}

// AsyncRequestsSender::Response::Response(ShardId shardId,
//                                         executor::RemoteCommandResponse response,
//                                         HostAndPort hp) {
//         shardId = std::move(shardId);
//         swResponse = std::move(response);
//         shardHostAndPort = std::move(hp);
//       }

AsyncRequestsSender::Response::Response(ShardId shardId,
                                        executor::RemoteCommandResponse response,
                                        std::pair<HostAndPort, HostAndPort> hp)
    : shardId(std::move(shardId)),
      swResponse(std::move(response)),
      DANSHostAndPorts(std::move(hp)) {}
// {
//   shardId = std::move(shardId);
//   swResponse = std::move(response);
//   DANSHostAndPorts = std::move(hp);
// }


AsyncRequestsSender::Response::Response(ShardId shardId,
                                        Status status,
                                        boost::optional<HostAndPort> hp)
    : shardId(std::move(shardId)), swResponse(std::move(status)), shardHostAndPort(std::move(hp)) {}

// AsyncRequestsSender::Response::Response(ShardId shardId,
//                                         Status status,
//                                         boost::optional<std::pair<HostAndPort, HostAndPort>> hp)
//     : shardId(std::move(shardId)), swResponse(std::move(status)), DANSHostAndPorts(std::move(hp)) {}

AsyncRequestsSender::RemoteData::RemoteData(ShardId shardId, BSONObj cmdObj)
    : shardId(std::move(shardId)), cmdObj(std::move(cmdObj)) {}


//SAM TODO: add an optional parameter?
Status AsyncRequestsSender::RemoteData::resolveShardIdToHostAndPort(
    const ReadPreferenceSetting& readPref) {
    const auto shard = getShard();
    if (!shard) {
        return Status(ErrorCodes::ShardNotFound,
                      str::stream() << "Could not find shard " << shardId);
    }

    // SAM: idea: modify readPref so it can capture if it's a duplicate
    // Then modify findHostWithMaxWait so it grabs the second option
    if (readPref.pref == ReadPreference::DuplicatePrimary) {
      // auto findHostStatus = shard->getTargeter()->getDualMatchingHosts(readPref);
      DANSHostAndPorts = shard->getTargeter()->getDualMatchingHosts(readPref);
      // SAM TODO: get status here

    } else {
      auto findHostStatus = shard->getTargeter()->findHostWithMaxWait(readPref, Seconds{20});
      if (!findHostStatus.isOK()) {
          return findHostStatus.getStatus();
      }
      shardHostAndPort = std::move(findHostStatus.getValue());
    }


    return Status::OK();
}

std::shared_ptr<Shard> AsyncRequestsSender::RemoteData::getShard() {
    // TODO: Pass down an OperationContext* to use here.
    return grid.shardRegistry()->getShardNoReload(shardId);
}

}  // namespace mongo
