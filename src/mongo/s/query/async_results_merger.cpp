/**
 *    Copyright (C) 2015 MongoDB Inc.
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

#include "mongo/s/query/async_results_merger.h"

#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/client/remote_command_targeter.h"
#include "mongo/db/pipeline/change_stream_constants.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/query/getmore_request.h"
#include "mongo/db/query/killcursors_request.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/remote_command_response.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

namespace mongo {

constexpr StringData AsyncResultsMerger::kSortKeyField;
const BSONObj AsyncResultsMerger::kWholeSortKeySortPattern = BSON(kSortKeyField << 1);

namespace {

// Maximum number of retries for network and replication notMaster errors (per host).
const int kMaxNumFailedHostRetryAttempts = 3;

/**
 * Returns the sort key out of the $sortKey metadata field in 'obj'. This object is of the form
 * {'': 'firstSortKey', '': 'secondSortKey', ...}.
 */
BSONObj extractSortKey(BSONObj obj, bool compareWholeSortKey) {
    auto key = obj[AsyncResultsMerger::kSortKeyField];
    invariant(key);
    if (compareWholeSortKey) {
        return key.wrap();
    }
    invariant(key.type() == BSONType::Object);
    return key.Obj();
}

/**
 * Returns an int less than 0 if 'leftSortKey' < 'rightSortKey', 0 if the two are equal, and an int
 * > 0 if 'leftSortKey' > 'rightSortKey' according to the pattern 'sortKeyPattern'.
 */
int compareSortKeys(BSONObj leftSortKey, BSONObj rightSortKey, BSONObj sortKeyPattern) {
    // This does not need to sort with a collator, since mongod has already mapped strings to their
    // ICU comparison keys as part of the $sortKey meta projection.
    const bool considerFieldName = false;
    return leftSortKey.woCompare(rightSortKey, sortKeyPattern, considerFieldName);
}

}  // namespace

AsyncResultsMerger::AsyncResultsMerger(OperationContext* opCtx,
                                       executor::TaskExecutor* executor,
                                       ClusterClientCursorParams* params)
    : _opCtx(opCtx),
      _executor(executor),
      _params(params),
      _mergeQueue(MergingComparator(_remotes, _params->sort, _params->compareWholeSortKey)) {
    size_t remoteIndex = 0;

    isDANS = _params->isDANS;
    if (_params->isDANS) {
      // SAM: emplace some DANS cursors if necessary
      for (const auto& DANSremote : _params->DANSRemotes) {
          // actually need to build out a little data here
          std::pair<NamespaceString,NamespaceString>
              nsses(DANSremote.cursorResponses.first.getNSS(),
                    DANSremote.cursorResponses.second.getNSS());

          std::pair<CursorId, CursorId>
              cids(DANSremote.cursorResponses.first.getCursorId(),
                   DANSremote.cursorResponses.second.getCursorId());

          //SAM: TODO: need to std::move?
          _DANSremotes.emplace_back(DANSremote.DANSHostAndPorts, nsses, cids);

          // We don't check the return value of _addBatchToBuffer here; if there was an error,
          // it will be stored in the remote and the first call to ready() will return true.
          _addBatchToBuffer(WithLock::withoutLock(), remoteIndex, DANSremote.cursorResponses.first);
          _addBatchToBuffer(WithLock::withoutLock(), remoteIndex, DANSremote.cursorResponses.second);
          ++remoteIndex;
      }


    } else {
      // SAM: alright another move in the right direction
      for (const auto& remote : _params->remotes) {
          _remotes.emplace_back(remote.hostAndPort,
                                remote.cursorResponse.getNSS(),
                                remote.cursorResponse.getCursorId());

          // We don't check the return value of _addBatchToBuffer here; if there was an error,
          // it will be stored in the remote and the first call to ready() will return true.
          _addBatchToBuffer(WithLock::withoutLock(), remoteIndex, remote.cursorResponse);
          ++remoteIndex;
      }
    }

    // Initialize command metadata to handle the read preference. We do this in case the readPref
    // is primaryOnly, in which case if the remote host for one of the cursors changes roles, the
    // remote will return an error.
    if (_params->readPreference) {
        _metadataObj = _params->readPreference->toContainingBSON();
    }
}

AsyncResultsMerger::~AsyncResultsMerger() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(_remotesExhausted(lk) || _lifecycleState == kKillComplete);
}

bool AsyncResultsMerger::remotesExhausted() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    return _remotesExhausted(lk);
}

// SAM: done
bool AsyncResultsMerger::_remotesExhausted(WithLock) {
    if (isDANS) {
        for (const auto& remote : _DANSremotes) {
            if (!remote.exhausted()) {
                return false;
            }
        }
    } else {
        for (const auto& remote : _remotes) {
            if (!remote.exhausted()) {
                return false;
            }
        }
    }

    return true;
}

//SAM done
Status AsyncResultsMerger::setAwaitDataTimeout(Milliseconds awaitDataTimeout) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    if (_params->tailableMode != TailableMode::kTailableAndAwaitData) {
        return Status(ErrorCodes::BadValue,
                      "maxTimeMS can only be used with getMore for tailable, awaitData cursors");
    }

    // For sorted tailable awaitData cursors on multiple shards, cap the getMore timeout at 1000ms.
    // This is to ensure that we get a continuous stream of updates from each shard with their most
    // recent optimes, which allows us to return sorted $changeStream results even if some shards
    // are yet to provide a batch of data. If the timeout specified by the client is greater than
    // 1000ms, then it will be enforced elsewhere.
    if (isDANS){
        _awaitDataTimeout = (!_params->sort.isEmpty() && _DANSremotes.size() > 1u
                                 ? std::min(awaitDataTimeout, Milliseconds{1000})
                                 : awaitDataTimeout);
    } else {
        _awaitDataTimeout = (!_params->sort.isEmpty() && _remotes.size() > 1u
                                 ? std::min(awaitDataTimeout, Milliseconds{1000})
                                 : awaitDataTimeout);
    }

    return Status::OK();
}

bool AsyncResultsMerger::ready() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    return _ready(lk);
}

void AsyncResultsMerger::detachFromOperationContext() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _opCtx = nullptr;
    // If we were about ready to return a boost::none because a tailable cursor reached the end of
    // the batch, that should no longer apply to the next use - when we are reattached to a
    // different OperationContext, it signals that the caller is ready for a new batch, and wants us
    // to request a new batch from the tailable cursor.
    _eofNext = false;
}

void AsyncResultsMerger::reattachToOperationContext(OperationContext* opCtx) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(!_opCtx);
    _opCtx = opCtx;
}

// SAM: TODO: error stuff
void AsyncResultsMerger::addNewShardCursors(
    const std::vector<ClusterClientCursorParams::RemoteCursor>& newCursors) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    //SAM: if the current result merger is doing DANS stuff we should error
    // Might actually not be the right approach though, maybe fallback?


    for (auto&& remote : newCursors) {
        _remotes.emplace_back(remote.hostAndPort,
                              remote.cursorResponse.getNSS(),
                              remote.cursorResponse.getCursorId());
    }
}

// SAM: TODO: more checking here
void AsyncResultsMerger::addNewShardCursors(
    const std::vector<ClusterClientCursorParams::DANSRemoteCursor>& newCursors) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    for (auto&& DANSremote : newCursors) {
        std::pair<NamespaceString,NamespaceString>
            nsses(DANSremote.cursorResponses.first.getNSS(),
                  DANSremote.cursorResponses.second.getNSS());

        std::pair<CursorId, CursorId>
            cids(DANSremote.cursorResponses.first.getCursorId(),
                 DANSremote.cursorResponses.second.getCursorId());

        //SAM: TODO: need to std::move?
        _DANSremotes.emplace_back(DANSremote.DANSHostAndPorts, nsses, cids);
    }
}

//SAM done
bool AsyncResultsMerger::_ready(WithLock lk) {
    if (_lifecycleState != kAlive) {
        return true;
    }

    if (_eofNext) {
        // Mark this operation as ready to return boost::none due to reaching the end of a batch of
        // results from a tailable cursor.
        return true;
    }


    if (isDANS) {
        for (const auto& remote : _DANSremotes) {
            // First check whether any of the remotes reported an error.
            if (!remote.status.isOK()) {
                _status = remote.status;
                return true;
            }
        }
    } else {
        for (const auto& remote : _remotes) {
            // First check whether any of the remotes reported an error.
            if (!remote.status.isOK()) {
                _status = remote.status;
                return true;
            }
        }
    }

    const bool hasSort = !_params->sort.isEmpty();
    return hasSort ? _readySorted(lk) : _readyUnsorted(lk);
}

// SAM done
bool AsyncResultsMerger::_readySorted(WithLock lk) {
    if (_params->tailableMode == TailableMode::kTailableAndAwaitData) {
        return _readySortedTailable(lk);
    }
    // Tailable non-awaitData cursors cannot have a sort.
    invariant(_params->tailableMode == TailableMode::kNormal);


    if (isDANS) {
        for (const auto& remote : _DANSremotes) {
            if (!remote.hasNext() && !remote.exhausted()) {
                return false;
            }
        }
    } else {
        for (const auto& remote : _remotes) {
            if (!remote.hasNext() && !remote.exhausted()) {
                return false;
            }
        }
    }

    return true;
}


bool AsyncResultsMerger::_readySortedTailable(WithLock) {
    if (_mergeQueue.empty()) {
        return false;
    }

    if (isDANS) {
        auto smallestRemote = _mergeQueue.top();
        auto smallestResult = _DANSremotes[smallestRemote].docBuffer.front();
        auto keyWeWantToReturn =
            extractSortKey(*smallestResult.getResult(), _params->compareWholeSortKey);
        for (const auto& remote : _DANSremotes) {
            if (!remote.promisedMinSortKey) {
                // In order to merge sorted tailable cursors, we need this value to be populated.
                return false;
            }
            if (compareSortKeys(keyWeWantToReturn, *remote.promisedMinSortKey, _params->sort) > 0) {
                // The key we want to return is not guaranteed to be smaller than future results from
                // this remote, so we can't yet return it.
                return false;
            }
        }
    } else {
        auto smallestRemote = _mergeQueue.top();
        auto smallestResult = _remotes[smallestRemote].docBuffer.front();
        auto keyWeWantToReturn =
            extractSortKey(*smallestResult.getResult(), _params->compareWholeSortKey);
        for (const auto& remote : _remotes) {
            if (!remote.promisedMinSortKey) {
                // In order to merge sorted tailable cursors, we need this value to be populated.
                return false;
            }
            if (compareSortKeys(keyWeWantToReturn, *remote.promisedMinSortKey, _params->sort) > 0) {
                // The key we want to return is not guaranteed to be smaller than future results from
                // this remote, so we can't yet return it.
                return false;
            }
        }
      }
    return true;
}

// SAM done
bool AsyncResultsMerger::_readyUnsorted(WithLock) {
    bool allExhausted = true;
    if (isDANS) {
        for (const auto& remote : _DANSremotes) {
            if (!remote.exhausted()) {
                allExhausted = false;
            }

            if (remote.hasNext()) {
                return true;
            }
        }
    } else {
        for (const auto& remote : _remotes) {
            if (!remote.exhausted()) {
                allExhausted = false;
            }

            if (remote.hasNext()) {
                return true;
            }
        }
    }

    return allExhausted;
}

// SAM nothing to do
StatusWith<ClusterQueryResult> AsyncResultsMerger::nextReady() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    dassert(_ready(lk));
    if (_lifecycleState != kAlive) {
        return Status(ErrorCodes::IllegalOperation, "AsyncResultsMerger killed");
    }

    if (!_status.isOK()) {
        return _status;
    }

    if (_eofNext) {
        _eofNext = false;
        return {ClusterQueryResult()};
    }

    const bool hasSort = !_params->sort.isEmpty();
    return hasSort ? _nextReadySorted(lk) : _nextReadyUnsorted(lk);
}

//SAM done
ClusterQueryResult AsyncResultsMerger::_nextReadySorted(WithLock) {
    // Tailable non-awaitData cursors cannot have a sort.
    invariant(_params->tailableMode != TailableMode::kTailable);

    if (_mergeQueue.empty()) {
        return {};
    }

    size_t smallestRemote = _mergeQueue.top();
    _mergeQueue.pop();

    ClusterQueryResult front;

    if (isDANS) {
        invariant(!_DANSremotes[smallestRemote].docBuffer.empty());
        invariant(_DANSremotes[smallestRemote].status.isOK());


        front = _DANSremotes[smallestRemote].docBuffer.front();
        _DANSremotes[smallestRemote].docBuffer.pop();

        // Re-populate the merging queue with the next result from 'smallestRemote', if it has a
        // next result.
        if (!_DANSremotes[smallestRemote].docBuffer.empty()) {
            _mergeQueue.push(smallestRemote);
        }
    } else {
        invariant(!_remotes[smallestRemote].docBuffer.empty());
        invariant(_remotes[smallestRemote].status.isOK());

        front = _remotes[smallestRemote].docBuffer.front();
        _remotes[smallestRemote].docBuffer.pop();

        // Re-populate the merging queue with the next result from 'smallestRemote', if it has a
        // next result.
        if (!_remotes[smallestRemote].docBuffer.empty()) {
            _mergeQueue.push(smallestRemote);
        }
    }

    return front;
}

// SAM done
ClusterQueryResult AsyncResultsMerger::_nextReadyUnsorted(WithLock) {
    size_t remotesAttempted = 0;
    if (isDANS) {
        while (remotesAttempted < _DANSremotes.size()) {
            // It is illegal to call this method if there is an error received from any shard.
            invariant(_DANSremotes[_gettingFromRemote].status.isOK());

            if (_DANSremotes[_gettingFromRemote].hasNext()) {
                ClusterQueryResult front = _DANSremotes[_gettingFromRemote].docBuffer.front();
                _DANSremotes[_gettingFromRemote].docBuffer.pop();

                if (_params->tailableMode == TailableMode::kTailable &&
                    !_DANSremotes[_gettingFromRemote].hasNext()) {
                    // The cursor is tailable and we're about to return the last buffered result. This
                    // means that the next value returned should be boost::none to indicate the end of
                    // the batch.
                    _eofNext = true;
                }

                return front;
            }

            // Nothing from the current remote so move on to the next one.
            ++remotesAttempted;
            if (++_gettingFromRemote == _DANSremotes.size()) {
                _gettingFromRemote = 0;
            }
        }
    } else {
        while (remotesAttempted < _remotes.size()) {
            // It is illegal to call this method if there is an error received from any shard.
            invariant(_remotes[_gettingFromRemote].status.isOK());

            if (_remotes[_gettingFromRemote].hasNext()) {
                ClusterQueryResult front = _remotes[_gettingFromRemote].docBuffer.front();
                _remotes[_gettingFromRemote].docBuffer.pop();

                if (_params->tailableMode == TailableMode::kTailable &&
                    !_remotes[_gettingFromRemote].hasNext()) {
                    // The cursor is tailable and we're about to return the last buffered result. This
                    // means that the next value returned should be boost::none to indicate the end of
                    // the batch.
                    _eofNext = true;
                }

                return front;
            }

            // Nothing from the current remote so move on to the next one.
            ++remotesAttempted;
            if (++_gettingFromRemote == _remotes.size()) {
                _gettingFromRemote = 0;
            }
        }
    }

    return {};
}

// SAM: done ish
Status AsyncResultsMerger::_askForNextBatch(WithLock, size_t remoteIndex) {
    if (isDANS) {
        auto& remote = _DANSremotes[remoteIndex];

        // SAM check both
        invariant(!remote.cbHandle.first.isValid());
        invariant(!remote.cbHandle.second.isValid());

        // If mongod returned less docs than the requested batchSize then modify the next getMore
        // request to fetch the remaining docs only. If the remote node has a plan with OR for top k and
        // a full sort as is the case for the OP_QUERY find then this optimization will prevent
        // switching to the full sort plan branch.
        auto adjustedBatchSize = _params->batchSize;
        if (_params->batchSize && *_params->batchSize > remote.fetchedCount) {
            adjustedBatchSize = *_params->batchSize - remote.fetchedCount;
        }

        // make two different commants?
        BSONObj cmdOb1 = GetMoreRequest(remote.cursorNss.first,
                                        remote.cursorId.first,
                                        adjustedBatchSize,
                                        _awaitDataTimeout,
                                        boost::none,
                                        boost::none)
                             .toBSON();

         BSONObj cmdOb2 = GetMoreRequest(remote.cursorNss.second,
                                         remote.cursorId.second,
                                         adjustedBatchSize,
                                         _awaitDataTimeout,
                                         boost::none,
                                         boost::none)
                              .toBSON();

        executor::RemoteCommandRequest primaryRequest(
            remote.getFirstTargetHost(), _params->nsString.db().toString(), cmdOb1, _metadataObj, _opCtx);

        executor::RemoteCommandRequest secondaryRequest(
            remote.getSecondTargetHost(), _params->nsString.db().toString(), cmdOb2, _metadataObj, _opCtx);

        // I need to remember to structure of this:
        // is this even right? Am I losing my mind????
        // std::pair<bool, mongo::StatusWith<mongo::executor::TaskExecutor::CallbackHandle> >
        std::pair<bool, mongo::StatusWith<mongo::executor::TaskExecutor::CallbackHandle> > callbackStatusPair =
            _executor->scheduleDANSRemoteCommand(primaryRequest, secondaryRequest,
                [this, remoteIndex](auto const& cbData) {
                    stdx::lock_guard<stdx::mutex> lk(this->_mutex);
                      this->_handleBatchResponse(lk, cbData, remoteIndex);
            });


        // check which one returned then check that callback status
        if (callbackStatusPair.first) {
          if (callbackStatusPair.second.isOK()) {
            // remote.cbHandle = callbackStatusPair.second.first.getValue();
            remote.cbHandle.first = callbackStatusPair.second.getValue();

          } else {
            return callbackStatusPair.second.getStatus();
          }

        } else {
          if (callbackStatusPair.second.isOK()) {
            // not the bool, the second
            // remote.cbHandle = callbackStatusPair.second.second.getValue();
            remote.cbHandle.second = callbackStatusPair.second.getValue();
          } else {
            return callbackStatusPair.second.getStatus();
          }

        }

        // SAM : TODO: make determination about primary or secondary based on the first item of the make_pair
        // if (callbackStatusPair.first) { // isPrimary
        //     // cancel the second host and port?
        //
        // } else {
        //
        // }
        // remote.cbHandle = callbackStatusPair.first.getValue();

    } else {
        auto& remote = _remotes[remoteIndex];

        invariant(!remote.cbHandle.isValid());

        // If mongod returned less docs than the requested batchSize then modify the next getMore
        // request to fetch the remaining docs only. If the remote node has a plan with OR for top k and
        // a full sort as is the case for the OP_QUERY find then this optimization will prevent
        // switching to the full sort plan branch.
        auto adjustedBatchSize = _params->batchSize;
        if (_params->batchSize && *_params->batchSize > remote.fetchedCount) {
            adjustedBatchSize = *_params->batchSize - remote.fetchedCount;
        }

        BSONObj cmdObj = GetMoreRequest(remote.cursorNss,
                                        remote.cursorId,
                                        adjustedBatchSize,
                                        _awaitDataTimeout,
                                        boost::none,
                                        boost::none)
                             .toBSON();

        executor::RemoteCommandRequest request(
            remote.getTargetHost(), _params->nsString.db().toString(), cmdObj, _metadataObj, _opCtx);

        auto callbackStatus =
            _executor->scheduleRemoteCommand(request, [this, remoteIndex](auto const& cbData) {
                stdx::lock_guard<stdx::mutex> lk(this->_mutex);
                this->_handleBatchResponse(lk, cbData, remoteIndex);
            });

        if (!callbackStatus.isOK()) {
            return callbackStatus.getStatus();
        }

        remote.cbHandle = callbackStatus.getValue();
    }
    return Status::OK();
}

/*
 * Note: When nextEvent() is called to do retries, only the remotes with retriable errors will
 * be rescheduled because:
 *
 * 1. Other pending remotes still have callback assigned to them.
 * 2. Remotes that already has some result will have a non-empty buffer.
 * 3. Remotes that reached maximum retries will be in 'exhausted' state.
 */
 //SAM done
StatusWith<executor::TaskExecutor::EventHandle> AsyncResultsMerger::nextEvent() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    if (_lifecycleState != kAlive) {
        // Can't schedule further network operations if the ARM is being killed.
        return Status(ErrorCodes::IllegalOperation,
                      "nextEvent() called on a killed AsyncResultsMerger");
    }

    if (_currentEvent.isValid()) {
        // We can't make a new event if there's still an unsignaled one, as every event must
        // eventually be signaled.
        return Status(ErrorCodes::IllegalOperation,
                      "nextEvent() called before an outstanding event was signaled");
    }

    // Schedule remote work on hosts for which we need more results.
    if (isDANS) {
        for (size_t i = 0; i < _DANSremotes.size(); ++i) {
            auto& remote = _DANSremotes[i];

            if (!remote.status.isOK()) {
                return remote.status;
            }

            if (!remote.hasNext() && !remote.exhausted() && (!remote.cbHandle.first.isValid() || !remote.cbHandle.second.isValid())) {
                // If this remote is not exhausted and there is no outstanding request for it, schedule
                // work to retrieve the next batch.
                auto nextBatchStatus = _askForNextBatch(lk, i);
                if (!nextBatchStatus.isOK()) {
                    return nextBatchStatus;
                }
            }
        }
    } else {
        for (size_t i = 0; i < _remotes.size(); ++i) {
            auto& remote = _remotes[i];

            if (!remote.status.isOK()) {
                return remote.status;
            }

            if (!remote.hasNext() && !remote.exhausted() && !remote.cbHandle.isValid()) {
                // If this remote is not exhausted and there is no outstanding request for it, schedule
                // work to retrieve the next batch.
                auto nextBatchStatus = _askForNextBatch(lk, i);
                if (!nextBatchStatus.isOK()) {
                    return nextBatchStatus;
                }
            }
        }
    }

    auto eventStatus = _executor->makeEvent();
    if (!eventStatus.isOK()) {
        return eventStatus;
    }
    auto eventToReturn = eventStatus.getValue();
    _currentEvent = eventToReturn;

    // It's possible that after we told the caller we had no ready results but before we replaced
    // _currentEvent with a new event, new results became available. In this case we have to signal
    // the new event right away to propagate the fact that the previous event had been signaled to
    // the new event.
    _signalCurrentEventIfReady(lk);
    return eventToReturn;
}

// SAM don't modify
StatusWith<CursorResponse> AsyncResultsMerger::_parseCursorResponse(
    const BSONObj& responseObj, const RemoteCursorData& remote) {

    auto getMoreParseStatus = CursorResponse::parseFromBSON(responseObj);
    if (!getMoreParseStatus.isOK()) {
        return getMoreParseStatus.getStatus();
    }

    auto cursorResponse = std::move(getMoreParseStatus.getValue());

    // If we get a non-zero cursor id that is not equal to the established cursor id, we will fail
    // the operation.
    if (cursorResponse.getCursorId() != 0 && remote.cursorId != cursorResponse.getCursorId()) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Expected cursorid " << remote.cursorId << " but received "
                                    << cursorResponse.getCursorId());
    }

    return std::move(cursorResponse);
}

// SAM new code
StatusWith<CursorResponse> AsyncResultsMerger::_parseCursorResponse(
    const BSONObj& responseObj, const DANSRemoteCursorData& remote) {

    auto getMoreParseStatus = CursorResponse::parseFromBSON(responseObj);
    if (!getMoreParseStatus.isOK()) {
        return getMoreParseStatus.getStatus();
    }

    auto cursorResponse = std::move(getMoreParseStatus.getValue());

    // If we get a non-zero cursor id that is not equal to the established cursor id, we will fail
    // the operation.


    bool validCursorId = false;
    // use the cursor id from the response to figure out which one it is
    // if they're the same... well that's a problem
    if (remote.cursorId.first == cursorResponse.getCursorId()) {
        validCursorId = true;
        cursorResponse.isPrimary = true;
    } else if (remote.cursorId.second == cursorResponse.getCursorId()) {
        validCursorId = true;
        cursorResponse.isPrimary = false;
    } // SAM: else should really error here

    if (!validCursorId) {
      return Status(ErrorCodes::BadValue,
                    str::stream() << "Expected cursorid " << remote.cursorId.first << " or "
                                  << remote.cursorId.second << " but received "
                                  << cursorResponse.getCursorId());
    }

    return std::move(cursorResponse);
}

void AsyncResultsMerger::updateRemoteMetadata(RemoteCursorData* remote,
                                              const CursorResponse& response) {
    // Update the cursorId; it is sent as '0' when the cursor has been exhausted on the shard.
    remote->cursorId = response.getCursorId();
    if (response.getLastOplogTimestamp() && !response.getLastOplogTimestamp()->isNull()) {
        // We only expect to see this for change streams.
        invariant(SimpleBSONObjComparator::kInstance.evaluate(_params->sort ==
                                                              change_stream_constants::kSortSpec));

        auto newLatestTimestamp = *response.getLastOplogTimestamp();
        if (remote->promisedMinSortKey) {
            auto existingLatestTimestamp = remote->promisedMinSortKey->firstElement().timestamp();
            if (existingLatestTimestamp == newLatestTimestamp) {
                // Nothing to update.
                return;
            }
            // The most recent oplog timestamp should never be smaller than the timestamp field of
            // the previous min sort key for this remote, if one exists.
            invariant(existingLatestTimestamp < newLatestTimestamp);
        }

        // Our new minimum promised sort key is the first key whose timestamp matches the most
        // recent reported oplog timestamp.
        auto newPromisedMin =
            BSON("" << *response.getLastOplogTimestamp() << "" << MINKEY << "" << MINKEY);

        // The promised min sort key should never be smaller than any results returned. If the
        // last entry in the batch is also the most recent entry in the oplog, then its sort key
        // of {lastOplogTimestamp, uuid, docID} will be greater than the artificial promised min
        // sort key of {lastOplogTimestamp, MINKEY, MINKEY}.
        auto maxSortKeyFromResponse =
            (response.getBatch().empty()
                 ? BSONObj()
                 : extractSortKey(response.getBatch().back(), _params->compareWholeSortKey));

        remote->promisedMinSortKey =
            (compareSortKeys(
                 newPromisedMin, maxSortKeyFromResponse, change_stream_constants::kSortSpec) < 0
                 ? maxSortKeyFromResponse.getOwned()
                 : newPromisedMin.getOwned());
    }
}

// SAM yikes, not 100% sure what to do here
void AsyncResultsMerger::updateRemoteMetadata(DANSRemoteCursorData* remote,
                                              const CursorResponse& response) {
    // Update the cursorId; it is sent as '0' when the cursor has been exhausted on the shard.
    // SAM: TODO: this might not be the right thing to do!
    if (response.isPrimary) {
        remote->cursorId.first = response.getCursorId();
    } else {
        remote->cursorId.second = response.getCursorId();
    }
    if (response.getLastOplogTimestamp() && !response.getLastOplogTimestamp()->isNull()) {
        // We only expect to see this for change streams.
        invariant(SimpleBSONObjComparator::kInstance.evaluate(_params->sort ==
                                                              change_stream_constants::kSortSpec));

        auto newLatestTimestamp = *response.getLastOplogTimestamp();
        if (remote->promisedMinSortKey) {
            auto existingLatestTimestamp = remote->promisedMinSortKey->firstElement().timestamp();
            if (existingLatestTimestamp == newLatestTimestamp) {
                // Nothing to update.
                return;
            }
            // The most recent oplog timestamp should never be smaller than the timestamp field of
            // the previous min sort key for this remote, if one exists.
            invariant(existingLatestTimestamp < newLatestTimestamp);
        }

        // Our new minimum promised sort key is the first key whose timestamp matches the most
        // recent reported oplog timestamp.
        auto newPromisedMin =
            BSON("" << *response.getLastOplogTimestamp() << "" << MINKEY << "" << MINKEY);

        // The promised min sort key should never be smaller than any results returned. If the
        // last entry in the batch is also the most recent entry in the oplog, then its sort key
        // of {lastOplogTimestamp, uuid, docID} will be greater than the artificial promised min
        // sort key of {lastOplogTimestamp, MINKEY, MINKEY}.
        auto maxSortKeyFromResponse =
            (response.getBatch().empty()
                 ? BSONObj()
                 : extractSortKey(response.getBatch().back(), _params->compareWholeSortKey));

        remote->promisedMinSortKey =
            (compareSortKeys(
                 newPromisedMin, maxSortKeyFromResponse, change_stream_constants::kSortSpec) < 0
                 ? maxSortKeyFromResponse.getOwned()
                 : newPromisedMin.getOwned());
    }
}

//SAM done
void AsyncResultsMerger::_handleBatchResponse(WithLock lk,
                                              CbData const& cbData,
                                              size_t remoteIndex) {
    // Got a response from remote, so indicate we are no longer waiting for one.
    if (isDANS) {
        _DANSremotes[remoteIndex].cbHandle = std::make_pair(executor::TaskExecutor::CallbackHandle(), executor::TaskExecutor::CallbackHandle());
    } else {
        _remotes[remoteIndex].cbHandle = executor::TaskExecutor::CallbackHandle();
    }

    //  On shutdown, there is no need to process the response.
    if (_lifecycleState != kAlive) {
        _signalCurrentEventIfReady(lk);  // First, wake up anyone waiting on '_currentEvent'.
        _cleanUpKilledBatch(lk);
        return;
    }
    try {
        _processBatchResults(lk, cbData.response, remoteIndex);
    } catch (DBException const& e) {
        if (isDANS) {
            _DANSremotes[remoteIndex].status = e.toStatus();
        } else {
            _remotes[remoteIndex].status = e.toStatus();
        }
    }
    _signalCurrentEventIfReady(lk);  // Wake up anyone waiting on '_currentEvent'.
}

void AsyncResultsMerger::_cleanUpKilledBatch(WithLock lk) {
    invariant(_lifecycleState == kKillStarted);

    // If this is the last callback to run then we are ready to free the ARM. We signal the
    // '_killCompleteEvent', which the caller of kill() may be waiting on.
    if (!_haveOutstandingBatchRequests(lk)) {
        // If the event is invalid then '_executor' is in shutdown, so we cannot signal events.
        if (_killCompleteEvent.isValid()) {
            _executor->signalEvent(_killCompleteEvent);
        }

        _lifecycleState = kKillComplete;
    }
}

// SAM done maybe
void AsyncResultsMerger::_cleanUpFailedBatch(WithLock lk, Status status, size_t remoteIndex) {
    if (isDANS) {
        auto& remote = _DANSremotes[remoteIndex];
        remote.status = std::move(status);
        // Unreachable host errors are swallowed if the 'allowPartialResults' option is set. We
        // remove the unreachable host entirely from consideration by marking it as exhausted.
        if (_params->isAllowPartialResults) {
            remote.status = Status::OK();

            // Clear the results buffer and cursor id.
            std::queue<ClusterQueryResult> emptyBuffer;
            std::swap(remote.docBuffer, emptyBuffer);
            remote.cursorId = std::make_pair(0,0);
        }
    } else {
        auto& remote = _remotes[remoteIndex];
        remote.status = std::move(status);
        // Unreachable host errors are swallowed if the 'allowPartialResults' option is set. We
        // remove the unreachable host entirely from consideration by marking it as exhausted.
        if (_params->isAllowPartialResults) {
            remote.status = Status::OK();

            // Clear the results buffer and cursor id.
            std::queue<ClusterQueryResult> emptyBuffer;
            std::swap(remote.docBuffer, emptyBuffer);
            remote.cursorId = 0;
        }
    }
}

// SAM done ish
void AsyncResultsMerger::_processBatchResults(WithLock lk,
                                              CbResponse const& response,
                                              size_t remoteIndex) {

    if (!response.isOK()) {
        _cleanUpFailedBatch(lk, response.status, remoteIndex);
        return;
    }
    if (isDANS) {
        auto& remote = _DANSremotes[remoteIndex];
        // SAM: do I need to do anything here?
        auto cursorResponseStatus = _parseCursorResponse(response.data, remote);
        if (!cursorResponseStatus.isOK()) {
            _cleanUpFailedBatch(lk, cursorResponseStatus.getStatus(), remoteIndex);
            return;
        }

        CursorResponse cursorResponse = std::move(cursorResponseStatus.getValue());

        if (cursorResponse.isPrimary) {
            remote.cursorId.first = cursorResponse.getCursorId();
        } else {
            remote.cursorId.second = cursorResponse.getCursorId();
        }

        // Save the batch in the remote's buffer.
        if (!_addBatchToBuffer(lk, remoteIndex, cursorResponse)) {
            return;
        }
        // If the cursor is tailable and we just received an empty batch, the next return value should
        // be boost::none in order to indicate the end of the batch. We do not ask for the next batch if
        // the cursor is tailable, as batches received from remote tailable cursors should be passed
        // through to the client as-is.
        // (Note: tailable cursors are only valid on unsharded collections, so the end of the batch from
        // one shard means the end of the overall batch).
        if (_params->tailableMode == TailableMode::kTailable && !remote.hasNext()) {
            invariant(_remotes.size() == 1);
            _eofNext = true;
        } else if (!remote.hasNext() && !remote.exhausted() && _lifecycleState == kAlive) {
            // If this is normal or tailable-awaitData cursor and we still don't have anything buffered
            // after receiving this batch, we can schedule work to retrieve the next batch right away.
            remote.status = _askForNextBatch(lk, remoteIndex);
        }

    } else {
        auto& remote = _remotes[remoteIndex];
        // SAM: do I need to do anything here?
        auto cursorResponseStatus = _parseCursorResponse(response.data, remote);
        if (!cursorResponseStatus.isOK()) {
            _cleanUpFailedBatch(lk, cursorResponseStatus.getStatus(), remoteIndex);
            return;
        }

        CursorResponse cursorResponse = std::move(cursorResponseStatus.getValue());
        remote.cursorId = cursorResponse.getCursorId();
        // Save the batch in the remote's buffer.
        if (!_addBatchToBuffer(lk, remoteIndex, cursorResponse)) {
            return;
        }
        // If the cursor is tailable and we just received an empty batch, the next return value should
        // be boost::none in order to indicate the end of the batch. We do not ask for the next batch if
        // the cursor is tailable, as batches received from remote tailable cursors should be passed
        // through to the client as-is.
        // (Note: tailable cursors are only valid on unsharded collections, so the end of the batch from
        // one shard means the end of the overall batch).
        if (_params->tailableMode == TailableMode::kTailable && !remote.hasNext()) {
            invariant(_remotes.size() == 1);
            _eofNext = true;
        } else if (!remote.hasNext() && !remote.exhausted() && _lifecycleState == kAlive) {
            // If this is normal or tailable-awaitData cursor and we still don't have anything buffered
            // after receiving this batch, we can schedule work to retrieve the next batch right away.
            remote.status = _askForNextBatch(lk, remoteIndex);
        }
    }
}

// sam i don't know here -> so I didn't do anything useful, great
bool AsyncResultsMerger::_addBatchToBuffer(WithLock lk,
                                           size_t remoteIndex,
                                           const CursorResponse& response) {
    if (isDANS) {
        auto& remote = _DANSremotes[remoteIndex];
        updateRemoteMetadata(&remote, response);
        for (const auto& obj : response.getBatch()) {
            // If there's a sort, we're expecting the remote node to have given us back a sort key.
            if (!_params->sort.isEmpty()) {
                auto key = obj[AsyncResultsMerger::kSortKeyField];
                if (!key) {
                    remote.status =
                        Status(ErrorCodes::InternalError,
                               str::stream() << "Missing field '" << AsyncResultsMerger::kSortKeyField
                                             << "' in document: "
                                             << obj);
                    return false;
                } else if (!_params->compareWholeSortKey && key.type() != BSONType::Object) {
                    remote.status =
                        Status(ErrorCodes::InternalError,
                               str::stream() << "Field '" << AsyncResultsMerger::kSortKeyField
                                             << "' was not of type Object in document: "
                                             << obj);
                    return false;
                }
            }

            ClusterQueryResult result(obj);
            remote.docBuffer.push(result);
            ++remote.fetchedCount;
        }

        // If we're doing a sorted merge, then we have to make sure to put this remote onto the
        // merge queue.
        if (!_params->sort.isEmpty() && !response.getBatch().empty()) {
            _mergeQueue.push(remoteIndex);
        }
        return true;
    } else {
        auto& remote = _remotes[remoteIndex];
        updateRemoteMetadata(&remote, response);
        for (const auto& obj : response.getBatch()) {
            // If there's a sort, we're expecting the remote node to have given us back a sort key.
            if (!_params->sort.isEmpty()) {
                auto key = obj[AsyncResultsMerger::kSortKeyField];
                if (!key) {
                    remote.status =
                        Status(ErrorCodes::InternalError,
                               str::stream() << "Missing field '" << AsyncResultsMerger::kSortKeyField
                                             << "' in document: "
                                             << obj);
                    return false;
                } else if (!_params->compareWholeSortKey && key.type() != BSONType::Object) {
                    remote.status =
                        Status(ErrorCodes::InternalError,
                               str::stream() << "Field '" << AsyncResultsMerger::kSortKeyField
                                             << "' was not of type Object in document: "
                                             << obj);
                    return false;
                }
            }

            ClusterQueryResult result(obj);
            remote.docBuffer.push(result);
            ++remote.fetchedCount;
        }

        // If we're doing a sorted merge, then we have to make sure to put this remote onto the
        // merge queue.
        if (!_params->sort.isEmpty() && !response.getBatch().empty()) {
            _mergeQueue.push(remoteIndex);
        }
        return true;
    }

}

//SAM nothing to do here
void AsyncResultsMerger::_signalCurrentEventIfReady(WithLock lk) {
    if (_ready(lk) && _currentEvent.isValid()) {
        // To prevent ourselves from signalling the event twice, we set '_currentEvent' as
        // invalid after signalling it.
        _executor->signalEvent(_currentEvent);
        _currentEvent = executor::TaskExecutor::EventHandle();
    }
}

//SAM done
bool AsyncResultsMerger::_haveOutstandingBatchRequests(WithLock) {
    if (isDANS) {
        for (const auto& remote : _DANSremotes) {
            if (remote.cbHandle.first.isValid() || remote.cbHandle.second.isValid()) {
                return true;
            }
        }
    } else {
        for (const auto& remote : _remotes) {
            if (remote.cbHandle.isValid()) {
                return true;
            }
        }
    }

    return false;
}

//SAM done
void AsyncResultsMerger::_scheduleKillCursors(WithLock, OperationContext* opCtx) {
    invariant(_killCompleteEvent.isValid());

    if (isDANS) {
        for (const auto& remote : _DANSremotes) {
            if (remote.status.isOK() && remote.cursorId.first && !remote.exhausted()) {
                BSONObj cmdObj = KillCursorsRequest(_params->nsString, {remote.cursorId.first}).toBSON();

                executor::RemoteCommandRequest request(
                    remote.getFirstTargetHost(), _params->nsString.db().toString(), cmdObj, opCtx);

                // Send kill request; discard callback handle, if any, or failure report, if not.
                _executor->scheduleRemoteCommand(request, [](auto const&) {}).getStatus().ignore();
            }
            if (remote.status.isOK() && remote.cursorId.second && !remote.exhausted()) {
                BSONObj cmdObj = KillCursorsRequest(_params->nsString, {remote.cursorId.second}).toBSON();

                executor::RemoteCommandRequest request(
                    remote.getSecondTargetHost(), _params->nsString.db().toString(), cmdObj, opCtx);

                // Send kill request; discard callback handle, if any, or failure report, if not.
                _executor->scheduleRemoteCommand(request, [](auto const&) {}).getStatus().ignore();
            }
        }
    } else {
        for (const auto& remote : _remotes) {
            if (remote.status.isOK() && remote.cursorId && !remote.exhausted()) {
                BSONObj cmdObj = KillCursorsRequest(_params->nsString, {remote.cursorId}).toBSON();

                executor::RemoteCommandRequest request(
                    remote.getTargetHost(), _params->nsString.db().toString(), cmdObj, opCtx);

                // Send kill request; discard callback handle, if any, or failure report, if not.
                _executor->scheduleRemoteCommand(request, [](auto const&) {}).getStatus().ignore();
            }
        }
    }
}

// SAM done
executor::TaskExecutor::EventHandle AsyncResultsMerger::kill(OperationContext* opCtx) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    if (_killCompleteEvent.isValid()) {
        invariant(_lifecycleState != kAlive);
        return _killCompleteEvent;
    }

    invariant(_lifecycleState == kAlive);
    _lifecycleState = kKillStarted;

    // Make '_killCompleteEvent', which we will signal as soon as all of our callbacks
    // have finished running.
    auto statusWithEvent = _executor->makeEvent();
    if (ErrorCodes::isShutdownError(statusWithEvent.getStatus().code())) {
        // The underlying task executor is shutting down.
        if (!_haveOutstandingBatchRequests(lk)) {
            _lifecycleState = kKillComplete;
        }
        return executor::TaskExecutor::EventHandle();
    }
    fassertStatusOK(28716, statusWithEvent);
    _killCompleteEvent = statusWithEvent.getValue();

    _scheduleKillCursors(lk, opCtx);

    if (!_haveOutstandingBatchRequests(lk)) {
        _lifecycleState = kKillComplete;
        // Signal the event right now, as there's nothing to wait for.
        _executor->signalEvent(_killCompleteEvent);
        return _killCompleteEvent;
    }

    _lifecycleState = kKillStarted;

    // Cancel all of our callbacks. Once they all complete, the event will be signaled
    if (isDANS) {
        for (const auto& remote : _DANSremotes) {
            if (remote.cbHandle.first.isValid()) {
                _executor->cancel(remote.cbHandle.first);
            }
            if (remote.cbHandle.second.isValid()) {
                _executor->cancel(remote.cbHandle.second);
            }
        }
    } else {
        for (const auto& remote : _remotes) {
            if (remote.cbHandle.isValid()) {
                _executor->cancel(remote.cbHandle);
            }
        }
    }
    return _killCompleteEvent;
}

//
// AsyncResultsMerger::RemoteCursorData
//

AsyncResultsMerger::RemoteCursorData::RemoteCursorData(HostAndPort hostAndPort,
                                                       NamespaceString cursorNss,
                                                       CursorId establishedCursorId)
    : cursorId(establishedCursorId),
      cursorNss(std::move(cursorNss)),
      shardHostAndPort(std::move(hostAndPort)) {}

const HostAndPort& AsyncResultsMerger::RemoteCursorData::getTargetHost() const {
    return shardHostAndPort;
}

bool AsyncResultsMerger::RemoteCursorData::hasNext() const {
    return !docBuffer.empty();
}

bool AsyncResultsMerger::RemoteCursorData::exhausted() const {
    return cursorId == 0;
}

//
// AsyncResultsMerger::DANSRemoteCursorData
//

// SAM: I have no idea if I really need this pair of nssstring
// I'm so tired of this it's horrible
AsyncResultsMerger::DANSRemoteCursorData::DANSRemoteCursorData(
    std::pair<HostAndPort, HostAndPort> hostAndPort,
    std::pair<NamespaceString, NamespaceString> cursorNss,
    std::pair<CursorId, CursorId> establishedCursorId)
    : cursorId(establishedCursorId),
      cursorNss(std::move(cursorNss)),
      shardHostAndPort(std::move(hostAndPort)) {}

const HostAndPort& AsyncResultsMerger::DANSRemoteCursorData::getFirstTargetHost() const {
    return shardHostAndPort.first;
}

const HostAndPort& AsyncResultsMerger::DANSRemoteCursorData::getSecondTargetHost() const {
    return shardHostAndPort.second;
}

bool AsyncResultsMerger::DANSRemoteCursorData::hasNext() const {
    return !docBuffer.empty();
}

bool AsyncResultsMerger::DANSRemoteCursorData::exhausted() const {
    bool exhausted = (cursorId.first == 0 && cursorId.second == 0);
    return exhausted;
}

//
// AsyncResultsMerger::MergingComparator
//

bool AsyncResultsMerger::MergingComparator::operator()(const size_t& lhs, const size_t& rhs) {
    const ClusterQueryResult& leftDoc = _remotes[lhs].docBuffer.front();
    const ClusterQueryResult& rightDoc = _remotes[rhs].docBuffer.front();

    return compareSortKeys(extractSortKey(*leftDoc.getResult(), _compareWholeSortKey),
                           extractSortKey(*rightDoc.getResult(), _compareWholeSortKey),
                           _sort) > 0;
}

void AsyncResultsMerger::blockingKill(OperationContext* opCtx) {
    auto killEvent = kill(opCtx);
    if (!killEvent) {
        // We are shutting down.
        return;
    }
    _executor->waitForEvent(killEvent);
}

StatusWith<ClusterQueryResult> AsyncResultsMerger::blockingNext() {
    while (!ready()) {
        auto nextEventStatus = nextEvent();
        if (!nextEventStatus.isOK()) {
            return nextEventStatus.getStatus();
        }
        auto event = nextEventStatus.getValue();

        // Block until there are further results to return.
        auto status = _executor->waitForEvent(_opCtx, event);

        if (!status.isOK()) {
            return status.getStatus();
        }

        // We have not provided a deadline, so if the wait returns without interruption, we do not
        // expect to have timed out.
        invariant(status.getValue() == stdx::cv_status::no_timeout);
    }

    return nextReady();
}

}  // namespace mongo
