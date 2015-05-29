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
			console.log('upgrade!!');
			var db = event.target.result;
			self._persistedCommits = db.createObjectStore('commits', {keyPath: 'id', unique: true});
			self._persistedCommits.createIndex('streamId', 'streamId', {unique:false});
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
	commit.isDispatched = false;
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
	var commits = this._db.transaction('commits', 'readwrite').objectStore('commits');
	commits.add(commit);

/*	this._commits.push(commit);
	this._commitIds.push(commit.id);
	this._commitConcurrencyCheck.push(concurrencyKey);
	var index = this._streamIndex[commit.streamId];
	if(!index) {
		index = this._streamIndex[commit.streamId] = [];
	}
	index.push(commit);
*/	
	return this._promisify(commit, callback);
};

IdbPartition.prototype.markAsDispatched = function(commit, callback) {
	commit.isDispatched = true;	
	return this._promisify(commit, callback);
};

IdbPartition.prototype.getUndispatched = function(callback) {
	var commits = this.queryAll;
	var undispatched = [];
	for(var i=0;i<commits.length; i++) {
		if(!commits[i].isDispatched) {
			undispatched.push(commit);
		}
	}
	return this._promisify(undispatched, callback);
};

IdbPartition.prototype.queryAll = function(callback) {
 	//return this._promisify(this._commits.slice(), callback);
 	var self = this;
 	var commits = [];
 	var commitsStore = this._db.transaction('commits', 'read').objectStore('commits');
	var cursor = commitsStore.openCursor();
	return new Promise(function(resolve, reject) {
		cursor.onerror = function(event) {
			reject(new Error(event));
		}
		cursor.onsucces = function(event) {
			console.log('success all');
			var c = event.target.result;
			if(c) {
				commits.push(c.value);
				c.continue();
			} else {
				resolve(commits);
			}
		}
	}).nodeify(callback);
};

IdbPartition.prototype.queryStream = function(streamId, callback) {
	console.log('queryStream ' + streamId);
 	var self = this;
 	var commits = [];
 	var commitsStore = this._db.transaction('commits', 'read').objectStore('commits');
	var cursor = commitsStore.index('streamId').openCursor();
	return new Promise(function(resolve, reject) {
		/*cursor.onerror = function(event) {
			console.log('error!!');
			reject(new Error(event));
		}*/
		resolve([]);
		cursor.onsucces = function(event) {
			console.log('success');
			var c = event.target.result;
			if(c) {
				commits.push(c.value);
				c.continue();
			} else {
				resolve(commits);
			}
		}
	}).nodeify(callback);
};


module.exports = IdbPartition;