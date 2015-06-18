var _ = require('lodash');
var Promise = require('bluebird');
var ConcurrencyError = require('tapeworm').ConcurrencyError;
var DuplicateCommitError = require('tapeworm').DuplicateCommitError;

var IdbPartition = function(idb, partitionId) {
	this._persistedCommits = null;
	this._streamIndex = {};
	this._commitIds = [];
	this._commitConcurrencyCheck = [];
	this._idb = idb;
	this._partitionId = partitionId;
}

IdbPartition.prototype._promisify = function(value, callback) {
	return Promise.resolve(value).nodeify(callback);
};

IdbPartition.prototype.open = function() {
	var self = this;
	var request = this._idb.open('tapeworm_'+this._partitionId, 1);
	return new Promise(function(resolve, reject) {
		request.onsuccess = function(event) {
			self._db = event.target.result;
			resolve(self);
		}
		request.onupgradeneeded = function(event) {
			var db = event.target.result;
			self._persistedCommits = db.createObjectStore('commits', {keyPath: 'id', unique: true});
			self._persistedCommits.createIndex('streamId', 'streamId', {unique:false});
			self._persistedCommits.createIndex('streamIdCommitSequence', 'streamIdCommitSequence', {unique:true});
			//db.createObjectStore('commits', {keyPath: 'id'});
		}
		request.onerror = function(event) {
			reject(new Error(event));
		}
		request.onblocked = function(event) {
			reject(new Error(event));
		}
	});
}

IdbPartition.prototype.append = function(commit, callback) {
	var self = this;
	commit.isDispatched = false;
	commit.streamIdCommitSequence = commit.streamId + commit.commitSequence;

	console.log(commit.streamIdCommitSequence);
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
	return new Promise(function(resolve, reject) {

		var txn = self._db.transaction(['commits'], 'readwrite');
		var commits = txn.objectStore('commits');

		txn.oncomplete = txn.onsuccess = function() {
			console.log('txn complete');
		}

		txn.onerror = function(event) {
			console.log(event);
			reject(new DuplicateCommitError(event));
		}

		var request = commits.add(commit);
		request.onsuccess = function() {
			console.log('transaction added');
			console.log(commit);
			process.nextTick(function() {
				resolve(commit)
			});
		};
		request.onerror = function(event) {
			console.log('error');
			console.log(error);
			reject(new Error(event));
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

IdbPartition.prototype.markAsDispatched = function(commit, callback) {
	throw Error('not implemented');
};

IdbPartition.prototype.getUndispatched = function(callback) {
	throw Error('not implemented');
};

IdbPartition.prototype.queryAll = function(callback) {
 	//return this._promisify(this._commits.slice(), callback);
 	var self = this;
 	var commits = [];
 	var commitsStore = this._db.transaction(['commits'], 'readonly').objectStore('commits');
	return new Promise(function(resolve, reject) {
		var cursor = commitsStore.openCursor();
		cursor.onerror = function(event) {
			reject(new Error(event));
		}
		cursor.onsuccess = function(event) {
			var c = event.target.result;
			if(c) {
				commits.push(c.value);
				c.continue();
			} else {
				//get indexeddb to sort
				resolve(_.sortBy(commits, 'commitSequence'));
			}
		}
	}).nodeify(callback);
};

IdbPartition.prototype.queryStream = function(streamId, callback) {
 	var self = this;
 	var commits = [];
	return new Promise(function(resolve, reject) {
//		resolve(commits);
		var txn = self._db.transaction(['commits'], 'readonly');
	 	var commitsStore = txn.objectStore('commits');
		var cursor = commitsStore.index('streamId').openCursor(streamId);
		cursor.onerror = function(event) {
			reject(new Error(event));
		}
		cursor.onsuccess = function(event) {
			var c = event.target.result;
			if(c) {
				commits.push(c.value);
				c.continue();
			} else {
				//get indexeddb to sort?
				resolve(_.sortBy(commits, 'commitSequence'));
			}
		}
	}).nodeify(callback);
};


module.exports = IdbPartition;