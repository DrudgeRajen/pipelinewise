import { call, put, select, takeLatest } from 'redux-saga/effects';
import { LOAD_STREAMS, UPDATE_STREAM_TO_REPLICATE } from './constants';
import {
  streamsLoaded,
  streamsLoadedError,
  updateStreamToReplicateDone,
  updateStreamToReplicateError
} from './actions';

import request from 'utils/request';

export function* getStreams(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/streams`;

  try {
    const streams = yield call(request, requestURL);
    yield put(streamsLoaded(streams));
  } catch (err) {
    yield put(streamsLoadedError(err));
  }
}

export function* updateStreamToReplicate(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/streams/${action.streamId}`;

  try {
    const response = yield call(request, requestURL, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(action.params),
    });
    yield put(updateStreamToReplicateDone(response));
  } catch (err) {
    yield put(updateStreamToReplicateError(err));
  }
}

export default function* tapPostgresData() {
  yield takeLatest(LOAD_STREAMS, getStreams);
  yield takeLatest(UPDATE_STREAM_TO_REPLICATE, updateStreamToReplicate);
}
