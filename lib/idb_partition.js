var _ = require('lodash');
var Promise = require('bluebird');
var ConcurrencyError = require('tapeworm').ConcurrencyError;
var DuplicateCommitError = require('tapeworm').DuplicateCommitError;

var IdbPartition = function (idb, partitionId, dbname) {
  this._idb = idb;
  this._partitionId = partitionId;
  this._dbname = dbname;
}

IdbPartition.prototype._promisify = function (value, callback) {
  return Promise.resolve(value).nodeify(callback);
};

IdbPartition.prototype.open = function () {
  var self = this;
  var request = this._idb.open('tw_' + this._dbname + '_' + this._partitionId, 3);
  return new Promise(function (resolve, reject) {
    request.onsuccess = function (event) {
      self._db = event.target.result;
      resolve(self);
    }
    request.onupgradeneeded = function (event) {
      var db = event.target.result;
      //			console.log(event);
      if (event.oldVersion < 1) {
        var persistedCommits = db.createObjectStore('commits', { keyPath: 'id', unique: true });
        persistedCommits.createIndex('streamId', 'streamId', { unique: false });
        persistedCommits.createIndex('streamIdCommitSequence', 'streamIdCommitSequence', { unique: true });
      }
      if (event.oldVersion < 2) {
        var truncatedCommits = db.createObjectStore('truncated', { keyPath: 'id', unique: true });
        truncatedCommits.createIndex('streamId', 'streamId', { unique: false });
      }
      if (event.oldVersion < 3) {
        var truncatedCommits = db.createObjectStore('snapshots', { keyPath: 'id', unique: true });
      }
      //db.createObjectStore('commits', {keyPath: 'id'});
    }
    request.onerror = function (event) {
      reject(new Error(event));
    }
    request.onblocked = function (event) {
      reject(new Error(event));
    }
  });
}


IdbPartition.prototype.truncateStreamFrom = function (streamId, commitSequence, remove, callback) {
  var self = this;
  if (_.isFunction(remove)) {
    callback = remove;
    remove = false;
  }
  return this.queryStream(streamId).then(function (commits) {
    return new Promise(function (resolve, reject) {
      var txn = self._db.transaction(["commits"], "readwrite");
      var txn2 = self._db.transaction(["truncated"], "readwrite");
      txn.oncomplete = txn.onsuccess = function () {
        resolve(self);
      }

      txn.onerror = function (event) {
        reject(new Error(event));
      }

      var commitStore = txn.objectStore("commits");
      var truncStore = txn2.objectStore("truncated");
      _.forEach(commits, function (commit) {
        if (commit.commitSequence >= commitSequence) {
          commitStore.delete(commit.id);
          if (!remove) {
            truncStore.add(commit);
          }
        }
      });
      resolve(self);
    });
  }).nodeify(callback);
}

IdbPartition.prototype.applyCommitHeader = function (commitId, header, callback) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var txn = self._db.transaction(["commits"], "readwrite");
    var commitStore = txn.objectStore("commits");
    var req = commitStore.get(commitId);
    req.onsuccess = function (event) {
      var c = event.target.result;
      if (c) {
        _.assign(c, header);
        var save = commitStore.put(c);
        save.onsuccess = function () {
          resolve(c);
        }
        save.onerror = function (error) {
          reject(error);
        }
      } else {
        reject(new Error("Unable to find commit: " + commitId));
      }
    };
    req.onerror = function (error) {
      console.log(commitId);
      reject(error);
    }
  });
}

IdbPartition.prototype.loadSnapshot = function (streamId, callback) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var txn = self._db.transaction(['snapshots'], 'readonly');
    var snaphots = txn.objectStore('snapshots');
    var request = snaphots.get(streamId);
    request.onsuccess = function (event) {
      resolve(request.result);
    }
    request.onerror = function (event) {
      reject(new Error('Unable to loadSnapshot ' + streamId));
    }
  }).nodeify(callback);
}

IdbPartition.prototype.storeSnapshot = function (streamId, snapshot, version, callback) {
  var self = this;
  return new Promise(function (resolve, reject) {

    var txn = self._db.transaction(['snapshots'], 'readwrite');
    var snapshots = txn.objectStore('snapshots');

    txn.oncomplete = txn.onsuccess = function () {
    }

    txn.onerror = function (event) {
      reject(new Error(event));
    }

    var request = snapshots.put({ id: streamId, version: version, snapshot: snapshot });
    request.onsuccess = function () {
      resolve(snapshot)
    };
    request.onerror = function (event) {
      reject(new Error(event));
    };
  });
}

IdbPartition.prototype.append = function (commit, callback) {
  var self = this;
  commit.isDispatched = false;
  commit.streamIdCommitSequence = commit.streamId + commit.commitSequence;

  //console.log(commit.streamIdCommitSequence);
  //check for duplicates
  /*	if(_.contains(this._commitIds, commit.id)) {
      throw new DuplicateCommitError("Duplicate commit of " + commit.id);
    }
    var concurrencyKey = commit.streamId+'-'+commit.commitSequence;
    if(_.contains(this._commitConcurrencyCheck, concurrencyKey)) {
      throw new ConcurrencyError('Concurrency error on stream ' + commit.streamId);
    }
  */
  //check concurrency
  return new Promise(function (resolve, reject) {

    var txn = self._db.transaction(['commits'], 'readwrite');
    var commits = txn.objectStore('commits');

    txn.oncomplete = txn.onsuccess = function () {
    }

    txn.onerror = function (event) {
      //console.log(event);
      reject(new DuplicateCommitError(event));
    }

    var request = commits.add(commit);
    request.onsuccess = function () {
      process.nextTick(function () {
        resolve(commit)
      });
    };
    request.onerror = function (event) {
      var req = commits.get(commit.id)
      req.onsuccess = function () {
        reject(new DuplicateCommitError(event));
      }
      req.onerror = function () {
        reject(new ConcurrencyError(event));
      }

    };
  });

  /*	this._commits.push(commit);
    this._commitIds.push(commit.id);
    this._commitConcurrencyCheck.push(concurrencyKey);
    var index = this._streamIndex[commit.streamId];
    if(!index) {
      index = this._streamIndex[commit.streamId] = [];
    }
    index.push(commit);
  */
  //return this._promisify(commit, callback);
};

IdbPartition.prototype.markAsDispatched = function (commit, callback) {
  throw Error('not implemented');
};

IdbPartition.prototype.getUndispatched = function (callback) {
  throw Error('not implemented');
};

IdbPartition.prototype.queryAll = function (callback) {
  //return this._promisify(this._commits.slice(), callback);
  var self = this;
  var commits = [];
  var commitsStore = this._db.transaction(['commits'], 'readonly').objectStore('commits');
  return new Promise(function (resolve, reject) {
    var cursor = commitsStore.openCursor();
    cursor.onerror = function (event) {
      reject(new Error(event));
    }
    cursor.onsuccess = function (event) {
      var c = event.target.result;
      if (c) {
        commits.push(c.value);
        c.continue();
      } else {
        //get indexeddb to sort
        resolve(_.sortBy(commits, 'commitSequence'));
      }
    }
  }).nodeify(callback);
};

IdbPartition.prototype.queryStream = function (streamId, fromEventSequence, callback) {
  if (_.isFunction(fromEventSequence)) {
    callback = fromEventSequence;
    fromEventSequence = 0;
  }
  var self = this;
  var commits = [];
  return new Promise(function (resolve, reject) {
    //		resolve(commits);
    var txn = self._db.transaction(['commits'], 'readonly');
    var commitsStore = txn.objectStore('commits');
    //	 	console.log(streamId);
    var cursor = commitsStore.index('streamId').openCursor(IDBKeyRange.only(streamId));
    cursor.onerror = function (event) {
      reject(new Error(event));
    }
    cursor.onsuccess = function (event) {
      var c = event.target.result;
      if (c) {
        commits.push(c.value);
        c.continue();
      } else {
        //get indexeddb to sort?
        var result = _.sortBy(commits, 'commitSequence');
        if (fromEventSequence > 0) {
          var startCommitId = 0;
          var foundEvents = 0;
          for (var i = 0; i < result.length; i++) {
            foundEvents += result[0].events.length;
            startCommitId++;
            if (foundEvents >= fromEventSequence) {
              break;
            }
          }
          var tooMany = foundEvents - fromEventSequence;

          result = result.slice(startCommitId - (tooMany > 0 ? 1 : 0));
          if (tooMany > 0) {
            result[0].events = result[0].events.slice(result[0].events.length - tooMany);
          }
        }
        resolve(result);
      }
    }
  }).nodeify(callback);
};


module.exports = IdbPartition;